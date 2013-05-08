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

package org.elasticsearch.index.cache.id.fst;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.cache.id.IdReaderCache;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class FSTIdCache extends AbstractIndexComponent implements IdCache, SegmentReader.CoreClosedListener {

    private final ConcurrentMap<Object, FSTIdReaderCache> idReaders;
    private final boolean reuse;

    IndexService indexService;

    @Inject
    public FSTIdCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        idReaders = ConcurrentCollections.newConcurrentMap();
        this.reuse = componentSettings.getAsBoolean("reuse", false);
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
        for (Iterator<FSTIdReaderCache> it = idReaders.values().iterator(); it.hasNext(); ) {
            FSTIdReaderCache idReaderCache = it.next();
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
        FSTIdReaderCache removed = idReaders.remove(reader.getCoreCacheKey());
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

                // first, go over and load all the id->doc map for all types
                for (AtomicReaderContext context : atomicReaderContexts) {
                    AtomicReader reader = context.reader();
                    if (idReaders.containsKey(reader.getCoreCacheKey())) {
                        // no need, continue
                        continue;
                    }

                    if (reader instanceof SegmentReader) {
                        ((SegmentReader) reader).addCoreClosedListener(this);
                    }
                    Map<String, TypeBuilder> readerBuilder = new HashMap<String, TypeBuilder>();
                    builders.put(reader.getCoreCacheKey(), readerBuilder);
                    cacheToReader.put(reader.getCoreCacheKey(), context.reader());


                    Terms terms = reader.terms(UidFieldMapper.NAME);
                    if (terms != null) {
                        TermsEnum termsEnum = terms.iterator(null);
                        DocsEnum docsEnum = null;
                        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                            HashedBytesArray[] typeAndId = Uid.splitUidIntoTypeAndId(term);
                            TypeBuilder typeBuilder = readerBuilder.get(typeAndId[0].toUtf8());
                            if (typeBuilder == null) {
                                typeBuilder = new TypeBuilder(reader);
                                readerBuilder.put(typeAndId[0].toUtf8(), typeBuilder);
                            }

                            HashedBytesArray idAsBytes = checkIfCanReuse(builders, typeAndId[1]);
                            docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, 0);
                            IntsRef scratchInts = new IntsRef();
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                typeBuilder.idToDoc.add(Util.toIntsRef(idAsBytes.toBytesRef(), scratchInts), (long) docId);
                                typeBuilder.docToId[docId] = idAsBytes;
                            }
                        }
                    }
                }

                // now, go and load the docId->parentId map

                for (AtomicReaderContext context : atomicReaderContexts) {
                    AtomicReader reader = context.reader();
                    if (idReaders.containsKey(reader.getCoreCacheKey())) {
                        // no need, continue
                        continue;
                    }

                    Map<String, TypeBuilder> readerBuilder = builders.get(reader.getCoreCacheKey());

                    Terms terms = reader.terms(ParentFieldMapper.NAME);
                    if (terms != null) {
                        TermsEnum termsEnum = terms.iterator(null);
                        DocsEnum docsEnum = null;
                        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                            HashedBytesArray[] typeAndId = Uid.splitUidIntoTypeAndId(term);

                            TypeBuilder typeBuilder = readerBuilder.get(typeAndId[0].toUtf8());
                            if (typeBuilder == null) {
                                typeBuilder = new TypeBuilder(reader);
                                readerBuilder.put(typeAndId[0].toUtf8(), typeBuilder);
                            }

                            HashedBytesArray idAsBytes = checkIfCanReuse(builders, typeAndId[1]);
                            boolean added = false; // optimize for when all the docs are deleted for this id

                            IntsRef scratchInts = new IntsRef();
                            docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, 0);
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                if (!added) {
//                                    typeBuilder.parentIdsValues.add(Util.toIntsRef(idAsBytes.toBytesRef(), scratchInts), (long) typeBuilder.t);
                                    added = true;
                                }
                                typeBuilder.parentIdsOrdinals.set(docId, typeBuilder.t);
                            }

                            if (added) {
                                typeBuilder.t++;
                            }
                        }
                    }
                }


                // now, build it back
                for (Map.Entry<Object, Map<String, TypeBuilder>> entry : builders.entrySet()) {
                    Object readerKey = entry.getKey();
                    MapBuilder<String, FSTReaderTypeCache> types = MapBuilder.newMapBuilder();
                    for (Map.Entry<String, TypeBuilder> typeBuilderEntry : entry.getValue().entrySet()) {
                        types.put(typeBuilderEntry.getKey(), new FSTReaderTypeCache(typeBuilderEntry.getKey(),
                                typeBuilderEntry.getValue().idToDoc.finish(),
                                typeBuilderEntry.getValue().docToId,
                                typeBuilderEntry.getValue().parentIdsValues.finish(),
                                typeBuilderEntry.getValue().parentIdsOrdinals));
                    }
                    IndexReader indexReader = cacheToReader.get(readerKey);
                    FSTIdReaderCache readerCache = new FSTIdReaderCache(types.immutableMap(), ShardUtils.extractShardId(indexReader));
                    idReaders.put(readerKey, readerCache);
                    onCached(readerCache);
                }
            }
        }
    }

    void onCached(FSTIdReaderCache readerCache) {
        if (readerCache.shardId != null) {
            IndexShard shard = indexService.shard(readerCache.shardId.id());
            if (shard != null) {
                shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    void onRemoval(FSTIdReaderCache readerCache) {
        if (readerCache.shardId != null) {
            IndexShard shard = indexService.shard(readerCache.shardId.id());
            if (shard != null) {
                shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    private HashedBytesArray checkIfCanReuse(Map<Object, Map<String, TypeBuilder>> builders, HashedBytesArray idAsBytes) {
        HashedBytesArray finalIdAsBytes;
        // go over and see if we can reuse this id
        if (reuse) {
            for (FSTIdReaderCache idReaderCache : idReaders.values()) {
                finalIdAsBytes = idReaderCache.canReuse(idAsBytes);
                if (finalIdAsBytes != null) {
                    return finalIdAsBytes;
                }
            }
        }
        // even if we don't enable reuse, at least check on the current "live" builders that we are handling
        for (Map<String, TypeBuilder> map : builders.values()) {
            for (TypeBuilder typeBuilder : map.values()) {
                finalIdAsBytes = typeBuilder.canReuse(idAsBytes);
                if (finalIdAsBytes != null) {
                    return finalIdAsBytes;
                }
            }
        }
        return idAsBytes;
    }

    private boolean refreshNeeded(List<AtomicReaderContext> atomicReaderContexts) {
        for (AtomicReaderContext atomicReaderContext : atomicReaderContexts) {
            if (!idReaders.containsKey(atomicReaderContext.reader().getCoreCacheKey())) {
                return true;
            }
        }
        return false;
    }

    static class TypeBuilder {

//        final ExtTObjectIntHasMap<HashedBytesArray> idToDoc = new ExtTObjectIntHasMap<HashedBytesArray>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
        final Builder<Long> idToDoc;
        final HashedBytesArray[] docToId;
//        final ArrayList<HashedBytesArray> parentIdsValues = new ArrayList<HashedBytesArray>();
        final Builder<BytesReference> parentIdsValues;
        final PackedInts.Mutable parentIdsOrdinals;
        int t = 1;  // current term number (0 indicated null value)

        IntsRef intsRef = new IntsRef(1);

        TypeBuilder(IndexReader reader) throws IOException {
            PositiveIntOutputs idToDocOutputs = PositiveIntOutputs.getSingleton(true);
            idToDoc = new Builder<Long>(FST.INPUT_TYPE.BYTE1, idToDocOutputs);

            parentIdsOrdinals = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
            BytesReferenceOutputs parentIdsOutputs = BytesReferenceOutputs.getSingleton(/*true*/);
            parentIdsValues = new Builder<BytesReference>(FST.INPUT_TYPE.BYTE1, parentIdsOutputs);
            // the first one indicates null value
            intsRef.ints[0] = 0;
            parentIdsValues.add(intsRef, null);
            docToId = new HashedBytesArray[reader.maxDoc()];
        }

        /**
         * Returns an already stored instance if exists, if not, returns null;
         */
        public HashedBytesArray canReuse(HashedBytesArray id) {
            return null;
//            return idToDoc.key(id);
        }
    }
}
