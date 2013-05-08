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

package org.elasticsearch.index.cache.id.paged;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class PagedIdCache extends AbstractIndexComponent implements IdCache, SegmentReader.CoreClosedListener {

    private final ConcurrentMap<Object, PagedIdReaderCache> idReaders;

    IndexService indexService;

    @Inject
    public PagedIdCache(Index index, @IndexSettings Settings indexSettings) {
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
        for (Iterator<PagedIdReaderCache> it = idReaders.values().iterator(); it.hasNext(); ) {
            PagedIdReaderCache idReaderCache = it.next();
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
        PagedIdReaderCache removed = idReaders.remove(reader.getCoreCacheKey());
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


                    Terms uidTerms = reader.terms(UidFieldMapper.NAME);
                    Terms parentUidTerms = reader.terms(ParentFieldMapper.NAME);

                    TermsEnum uidTermsEnum = uidTerms.iterator(null);
                    TermsEnum parentUidTermsEnum = parentUidTerms.iterator(null);
                    DocsEnum docsEnum = null;
                    while (true) {
                        BytesRef uidAndType = uidTermsEnum.next();
                        BytesRef parentUidAndType = parentUidTermsEnum.next();
                        if (uidAndType == null && parentUidAndType == null) {
                            break;
                        } else if (uidAndType == null) {
                            for (; parentUidAndType != null; parentUidAndType = parentUidTermsEnum.next()) {
                                Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, parentUidAndType);
                                BytesRef parentUid = idAndType.v1();
                                TypeBuilder typeBuilder = idAndType.v2();
                                long offset = typeBuilder.parentIds.copyUsingLengthPrefix(parentUid);
                                docsEnum = uidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                    typeBuilder.childDocIdToParentIdOffsetWriter.set(docId, offset);
                                }
                            }
                            break;
                        } else if (parentUidAndType == null) {
                            for (; uidAndType != null; uidAndType = uidTermsEnum.next()) {
                                Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, uidAndType);
                                BytesRef uid = idAndType.v1();
                                TypeBuilder typeBuilder = idAndType.v2();
                                long offset = typeBuilder.parentIds.copyUsingLengthPrefix(uid);
                                docsEnum = uidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                    typeBuilder.parentDocIdToUidOffsetWriter.set(docId, offset);
                                }
                            }
                            break;
                        }

                        BytesRef uid = Uid.splitUidIntoTypeAndId_br(uidAndType)[1];
                        BytesRef parentUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType)[1];

                        int cmp = uid.compareTo(parentUid);
                        if (cmp < 0) {
                            long offset;
                            do {
                                Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, uidAndType);
                                uid = idAndType.v1();
                                TypeBuilder typeBuilder = idAndType.v2();
                                offset = typeBuilder.parentIds.copyUsingLengthPrefix(uid);
                                docsEnum = uidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                    typeBuilder.parentDocIdToUidOffsetWriter.set(docId, offset);
                                }
                                uidAndType = uidTermsEnum.next();
                                if (uidAndType != null) {
                                    uid = Uid.splitUidIntoTypeAndId_br(uidAndType)[1];
                                } else {
                                    uid = null;
                                }
                            } while (uid != null && uid.compareTo(parentUid) < 0);
                            // Now catch up in the parent_uid field
                            Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, parentUidAndType);
                            TypeBuilder typeBuilder = idAndType.v2();
                            docsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                typeBuilder.childDocIdToParentIdOffsetWriter.set(docId, offset);
                            }
                        } else if (cmp == 0) {
                            Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, parentUidAndType);
                            uid = idAndType.v1();
                            TypeBuilder typeBuilder = idAndType.v2();
                            long offset = typeBuilder.parentIds.copyUsingLengthPrefix(uid);
                            docsEnum = uidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                typeBuilder.parentDocIdToUidOffsetWriter.set(docId, offset);
                            }
                            docsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                typeBuilder.childDocIdToParentIdOffsetWriter.set(docId, offset);
                            }
                        } else if (cmp > 0) {
                            long offset;
                            do {
                                Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, parentUidAndType);
                                BytesRef parentId = idAndType.v1();
                                TypeBuilder typeBuilder = idAndType.v2();

                                offset = typeBuilder.parentIds.copyUsingLengthPrefix(parentId);
                                docsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                    typeBuilder.childDocIdToParentIdOffsetWriter.set(docId, offset);
                                }
                                parentUidAndType = parentUidTermsEnum.next();
                                if (parentUidAndType != null) {
                                    parentUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType)[1];
                                } else {
                                    parentUid = null;
                                }
                            } while (parentUidAndType != null && parentUid.compareTo(uid) < 0);
                            // Now catch up in the uid field
                            Tuple<BytesRef, TypeBuilder> idAndType = resolveUidAndTypeBuilder(reader, readerBuilder, uidAndType);
                            TypeBuilder typeBuilder = idAndType.v2();
                            docsEnum = uidTermsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                                typeBuilder.parentDocIdToUidOffsetWriter.set(docId, offset);
                            }
                        }
                    }
                }

                // now, build it back
                for (Map.Entry<Object, Map<String, TypeBuilder>> entry : builders.entrySet()) {
                    Object readerKey = entry.getKey();
                    MapBuilder<String, PagedIdReaderTypeCache> types = MapBuilder.newMapBuilder();
                    for (Map.Entry<String, TypeBuilder> typeBuilderEntry : entry.getValue().entrySet()) {
                        types.put(typeBuilderEntry.getKey(), new PagedIdReaderTypeCache(typeBuilderEntry.getKey(),
                                typeBuilderEntry.getValue().parentIds.freeze(true),
                                0l, // TODO
                                typeBuilderEntry.getValue().childDocIdToParentIdOffsetWriter.getMutable(),
                                typeBuilderEntry.getValue().parentDocIdToUidOffsetWriter.getMutable()));
                    }
                    IndexReader indexReader = cacheToReader.get(readerKey);
                    PagedIdReaderCache readerCache = new PagedIdReaderCache(types.immutableMap(), ShardUtils.extractShardId(indexReader));
                    idReaders.put(readerKey, readerCache);
                    onCached(readerCache);
                }
            }
        }
    }

    void onCached(PagedIdReaderCache readerCache) {
        if (readerCache.shardId != null) {
            IndexShard shard = indexService.shard(readerCache.shardId.id());
            if (shard != null) {
                shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    void onRemoval(PagedIdReaderCache readerCache) {
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

    private Tuple<BytesRef, TypeBuilder> resolveUidAndTypeBuilder(IndexReader reader, Map<String, TypeBuilder> readerBuilder, BytesRef uid) {
        BytesRef[] typeAndId = Uid.splitUidIntoTypeAndId_br(uid);
        String type = typeAndId[0].utf8ToString();
        TypeBuilder typeBuilder = readerBuilder.get(type);
        if (typeBuilder == null) {
            typeBuilder = new TypeBuilder(reader);
            readerBuilder.put(type, typeBuilder);
        }
        return new Tuple<BytesRef, TypeBuilder>(typeAndId[1], typeBuilder);
    }

    static class TypeBuilder {

        final PagedBytes parentIds = new PagedBytes(15);
        final GrowableWriter parentDocIdToUidOffsetWriter;
        final GrowableWriter childDocIdToParentIdOffsetWriter;

        TypeBuilder(IndexReader reader) {
            parentIds.copyUsingLengthPrefix(new BytesRef()); // pointer 0 is for not set
            parentDocIdToUidOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
            childDocIdToParentIdOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
        }
    }
}
