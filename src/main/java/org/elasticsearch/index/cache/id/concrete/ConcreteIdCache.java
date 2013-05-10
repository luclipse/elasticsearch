/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.id.concrete;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.cache.id.IdReaderCache;
import org.elasticsearch.index.cache.id.paged.ParentChildUidIterator;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ConcreteIdCache extends AbstractIndexComponent implements IdCache, SegmentReader.CoreClosedListener {

    static final BytesRef EMPTY = new BytesRef();

    private final ConcurrentMap<Object, ConcreteReaderCache> idReaders;

    IndexService indexService;

    @Inject
    public ConcreteIdCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        idReaders = ConcurrentCollections.newConcurrentMap();
    }

    @Override
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public void close() throws ElasticSearchException {
        clear();
    }

    @Override
    public void clear() {
        for (Iterator<ConcreteReaderCache> it = idReaders.values().iterator(); it.hasNext(); ) {
            ConcreteReaderCache idReaderCache = it.next();
            it.remove();
            onRemoval(idReaderCache);
        }
    }

    @Override
    public void onClose(SegmentReader owner) {
        clear(owner);
    }

    @Override
    public void clear(IndexReader reader) {
        ConcreteReaderCache removed = idReaders.remove(reader.getCoreCacheKey());
        if (removed != null) onRemoval(removed);
    }

    @Override
    public IdReaderCache reader(AtomicReader reader) {
        return idReaders.get(reader.getCoreCacheKey());
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Iterator<IdReaderCache> iterator() {
        return (Iterator<IdReaderCache>) idReaders.values();
    }

    @SuppressWarnings({"StringEquality"})
    @Override
    public void refresh(List<AtomicReaderContext> atomicReaderContexts) throws Exception {
        // do a quick check for the common case, that all are there
        if (refreshNeeded(atomicReaderContexts)) {
            synchronized (idReaders) {
                if (!refreshNeeded(atomicReaderContexts)) {
                    return;
                }

                // do the refresh
                Map<Object, Map<String, TypeBuilder>> builders = new HashMap<Object, Map<String, TypeBuilder>>();
                Map<Object, IndexReader> cacheToReader = new HashMap<Object, IndexReader>();

                // We don't want to load uid of child documents, this allows us to not load uids of child types.
                Set<String> parentTypes = new HashSet<String>();
                for (String type : indexService.mapperService().types()) {
                    ParentFieldMapper parentFieldMapper = indexService.mapperService().documentMapper(type).parentFieldMapper();
                    if (parentFieldMapper != null) {
                        parentTypes.add(parentFieldMapper.type());
                    }
                }
                ParentChildUidIterator iterator = new ParentChildUidIterator(parentTypes);

                // first, go over and load all the id->doc map for all types
                for (AtomicReaderContext context : atomicReaderContexts) {
                    final AtomicReader reader = context.reader();
                    if (idReaders.containsKey(reader.getCoreCacheKey())) {
                        // no need, continue
                        continue;
                    }

                    if (reader instanceof SegmentReader) {
                        ((SegmentReader) reader).addCoreClosedListener(this);
                    }
                    final Map<String, TypeBuilder> readerBuilder = new HashMap<String, TypeBuilder>();
                    builders.put(reader.getCoreCacheKey(), readerBuilder);
                    cacheToReader.put(reader.getCoreCacheKey(), context.reader());
                    iterator.iterate(reader, new ParentChildUidIterator.Callback() {

                        @Override
                        public void onUid(String type, BytesRef uid, DocsEnum parentDocs, DocsEnum childDocs) throws IOException {
                            TypeBuilder typeBuilder = resolveTypeBuilder(reader, readerBuilder, type);
                            typeBuilder.parentIds.add(uid);
                            long ord = typeBuilder.parentIds.size() - 1;
                            if (parentDocs != null) {
                                for (int docId = parentDocs.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = parentDocs.nextDoc()) {
                                    typeBuilder.docIdToUidOffsetWriter.set(docId, ord);
                                }
                            }
                            if (childDocs != null) {
                                for (int docId = childDocs.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = childDocs.nextDoc()) {
                                    typeBuilder.docIdToParentUidOffsetWriter.set(docId, ord);
                                }
                            }
                        }

                    });
                }

                // now, build it back
                for (Map.Entry<Object, Map<String, TypeBuilder>> entry : builders.entrySet()) {
                    Object readerKey = entry.getKey();
                    MapBuilder<String, ConcreteIdReaderTypeCache> types = MapBuilder.newMapBuilder();
                    for (Map.Entry<String, TypeBuilder> typeBuilderEntry : entry.getValue().entrySet()) {
                        types.put(typeBuilderEntry.getKey(), new ConcreteIdReaderTypeCache(typeBuilderEntry.getKey(),
                                typeBuilderEntry.getValue().parentIds.toArray(new BytesRef[typeBuilderEntry.getValue().parentIds.size()]),
                                typeBuilderEntry.getValue().docIdToParentUidOffsetWriter.getMutable(),
                                typeBuilderEntry.getValue().docIdToUidOffsetWriter.getMutable()));
                    }
                    IndexReader indexReader = cacheToReader.get(readerKey);
                    ConcreteReaderCache readerCache = new ConcreteReaderCache(types.immutableMap(), ShardUtils.extractShardId(indexReader));
                    idReaders.put(readerKey, readerCache);
                    onCached(readerCache);
                }
            }
        }
    }

    void onCached(ConcreteReaderCache readerCache) {
        if (readerCache.shardId != null) {
            IndexShard shard = indexService.shard(readerCache.shardId.id());
            if (shard != null) {
                shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    void onRemoval(ConcreteReaderCache readerCache) {
        if (readerCache.shardId != null) {
            IndexShard shard = indexService.shard(readerCache.shardId.id());
            if (shard != null) {
                shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    private boolean refreshNeeded(List<AtomicReaderContext> atomicReaderContexts) {
        for (AtomicReaderContext atomicReaderContext : atomicReaderContexts) {
            if (!idReaders.containsKey(atomicReaderContext.reader().getCoreCacheKey())) {
                return true;
            }
        }
        return false;
    }

    private TypeBuilder resolveTypeBuilder(IndexReader reader, Map<String, TypeBuilder> readerBuilder, String type) {
        TypeBuilder typeBuilder = readerBuilder.get(type);
        if (typeBuilder == null) {
            typeBuilder = new TypeBuilder(reader);
            readerBuilder.put(type, typeBuilder);
        }
        return typeBuilder;
    }

    static class TypeBuilder {

        final List<BytesRef> parentIds = new ArrayList<BytesRef>();
        final GrowableWriter docIdToUidOffsetWriter;
        final GrowableWriter docIdToParentUidOffsetWriter;

        TypeBuilder(IndexReader reader) {
            parentIds.add(null); // pointer 0 is for not set
            docIdToUidOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
            docIdToParentUidOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
        }
    }
}
