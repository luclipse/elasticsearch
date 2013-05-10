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

package org.elasticsearch.index.parentdata;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 */
public abstract class ParentData<Atomic extends AtomicParentData, PB extends ParentData.ParentBuilder<Atomic>> implements SegmentReader.CoreClosedListener {

    private final ConcurrentMap<Object, Atomic> atomicMap = ConcurrentCollections.newConcurrentMap();
    IndexService indexService;

    // we need to "inject" the index service to not create cyclic dep
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    public void close() throws ElasticSearchException {
        clear();
    }

    public void clear() {
        for (Iterator<Atomic> it = atomicMap.values().iterator(); it.hasNext(); ) {
            Atomic parentAtomicData = it.next();
            it.remove();
            onRemoval(parentAtomicData);
        }
    }

    @Override
    public void onClose(SegmentReader owner) {
        clear(owner);
    }

    public void clear(IndexReader reader) {
        Atomic removed = atomicMap.remove(reader.getCoreCacheKey());
        if (removed != null) onRemoval(removed);
    }

    public Atomic atomic(AtomicReader reader) {
        return atomicMap.get(reader.getCoreCacheKey());
    }

    void onCached(Atomic atomicParentData) {
        if (atomicParentData.shardId() != null) {
            IndexShard shard = indexService.shard(atomicParentData.shardId().id());
            if (shard != null) {
                //shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    void onRemoval(Atomic atomicParentData) {
        if (atomicParentData.shardId() != null) {
            IndexShard shard = indexService.shard(atomicParentData.shardId().id());
            if (shard != null) {
                //shard.idCache().onCached(readerCache.sizeInBytes());
            }
        }
    }

    public void refresh(List<AtomicReaderContext> readers) throws Exception {
        // do a quick check for the common case, that all are there
        if (refreshNeeded(readers)) {
            synchronized (atomicMap) {
                if (!refreshNeeded(readers)) {
                    return;
                }

                // do the refresh
                Map<Object, PB> builders = new HashMap<Object, PB>();
                Map<Object, AtomicReader> cacheToReader = new HashMap<Object, AtomicReader>();

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
                for (AtomicReaderContext context : readers) {
                    final AtomicReader reader = context.reader();
                    if (atomicMap.containsKey(reader.getCoreCacheKey())) {
                        // no need, continue
                        continue;
                    }

                    if (reader instanceof SegmentReader) {
                        ((SegmentReader) reader).addCoreClosedListener(this);
                    }
                    final PB parentBuilder = newParentBuilder(reader);
                    builders.put(reader.getCoreCacheKey(), parentBuilder);
                    cacheToReader.put(reader.getCoreCacheKey(), context.reader());

                    iterator.iterate(reader, parentBuilder);
                }

                // now, build it back
                for (Map.Entry<Object, PB> entry : builders.entrySet()) {
                    Object readerKey = entry.getKey();
                    AtomicReader reader = cacheToReader.get(readerKey);
                    Atomic atomicParentData = entry.getValue().build(reader);
                    atomicMap.put(readerKey, atomicParentData);
                    onCached(atomicParentData);
                }
            }
        }

    }

    protected abstract PB newParentBuilder(AtomicReader reader);

    private boolean refreshNeeded(List<AtomicReaderContext> atomicReaderContexts) {
        for (AtomicReaderContext atomicReaderContext : atomicReaderContexts) {
            if (!atomicMap.containsKey(atomicReaderContext.reader().getCoreCacheKey())) {
                return true;
            }
        }
        return false;
    }

    protected static abstract class ParentBuilder<T extends AtomicParentData> implements ParentChildUidIterator.Callback {
        public abstract T build(AtomicReader reader);
    }
}
