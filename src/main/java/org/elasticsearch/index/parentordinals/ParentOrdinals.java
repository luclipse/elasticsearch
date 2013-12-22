/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.parentordinals;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 */
public abstract class ParentOrdinals {

    protected final Map<Object, Map<String, Segment>> readerToTypes;

    protected ParentOrdinals(Map<Object, Map<String, Segment>> readerToTypes) {
        this.readerToTypes = readerToTypes;
    }

    public final Segment ordinals(String type, AtomicReaderContext context) throws IOException {
        AtomicReader reader = context.reader();
        Map<String, Segment> segmentValues = readerToTypes.get(reader.getCoreCacheKey());
        if (segmentValues == null) {
            return null;
        }

        return segmentValues.get(type);
    }

    public abstract boolean supportsValueLookup();

    public abstract BytesRef parentValue(int ordinal, BytesRef spare);

    public static abstract class Builder<T extends ParentOrdinals> extends AbstractIndexShardComponent {

        protected Builder(ShardId shardId, @IndexSettings Settings indexSettings) {
            super(shardId, indexSettings);
        }

        public T build(T current, IndicesWarmer.WarmerContext context, NavigableSet<BytesRef> parentTypes) throws IOException {
            if (current != null) {
                IndexReader indexReader = context.completeSearcher() != null ? context.completeSearcher().reader() : context.newSearcher().reader();
                boolean doLoad = false;
                for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
                    if (!current.readerToTypes.containsKey(atomicReaderContext.reader().getCoreCacheKey())) {
                        doLoad = true;
                    }
                }
                if (doLoad) {
                    return doBuild(current, context, parentTypes);
                } else {
                    return current;
                }
            } else {
                return doBuild(current, context, parentTypes);
            }
        }

        public abstract T doBuild(T current, IndicesWarmer.WarmerContext warmerContext, NavigableSet<BytesRef> parentTypes) throws IOException;

    }

    public final static class Segment {

        private final PackedInts.Reader ordinals;

        private Segment(PackedInts.Reader ordinals) {
            this.ordinals = ordinals;
        }

        public int ordinal(int doc) {
            return (int) ordinals.get(doc);
        }

        public boolean isEmpty() {
            return ordinals.getBitsPerValue() == 0;
        }

        public void forEachOrdinal(OnOrdinal onOrdinal) {
            for (int docId = 0; docId < ordinals.size(); docId++) {
                onOrdinal.onOrdinal((int) ordinals.get(docId));
            }
        }

        public final static class Builder {

            private final int maxDoc;
            private GrowableWriter ordinals;

            public Builder(int maxDoc) {
                this.maxDoc = maxDoc;
            }

            public void set(int doc, int ordinal) {
                if (ordinals == null) {
                    ordinals = new GrowableWriter(1, maxDoc, PackedInts.FAST);
                }
                this.ordinals.set(doc, ordinal);
            }

            public Segment build() {
                return new Segment(ordinals != null ? ordinals.getMutable() : new PackedInts.NullReader(maxDoc));
            }

        }

        public interface OnOrdinal {

            public void onOrdinal(int ordinal);

        }
    }

}
