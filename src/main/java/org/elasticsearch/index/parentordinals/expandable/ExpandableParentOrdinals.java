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

package org.elasticsearch.index.parentordinals.expandable;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.index.parentordinals.TermsEnums;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

/**
 */
public class ExpandableParentOrdinals extends ParentOrdinals implements SegmentReader.CoreClosedListener {

    private final ESLogger logger;

    // Can't hold more than 2G of parent ids..
    private final BytesRefHash parentValues;
    private final IntArrayList ordinalUsages;
    private final double optimizeParentValuesRatio;

    private volatile boolean rebuild;
    private volatile int unusedOrdinals;

    public ExpandableParentOrdinals(ESLogger logger, double optimizeParentValuesRatio) {
        super(ConcurrentCollections.<Object, Map<String, Segment>>newConcurrentMap());
        this.logger = logger;
        this.parentValues = new BytesRefHash();
        this.parentValues.add(new BytesRef(BytesRef.EMPTY_BYTES));
        this.ordinalUsages = new IntArrayList();
        this.ordinalUsages.add(0);
        this.optimizeParentValuesRatio = optimizeParentValuesRatio;
    }

    @Override
    public boolean supportsValueLookup() {
        return true;
    }

    public BytesRef parentValue(int ordinal, BytesRef spare) {
        return parentValues.get(ordinal, spare);
    }

    @Override
    public void onClose(Object owner) {
        Map<String, Segment> segmentValues = readerToTypes.remove(owner);
        if (segmentValues == null) {
            return;
        }

        if (rebuild) {
            return;
        }

        synchronized (readerToTypes) {
            for (Segment segment : segmentValues.values()) {
                segment.forEachOrdinal(new Segment.OnOrdinal() {
                    @Override
                    public void onOrdinal(int ordinal) {
                        if (ordinal == 0) {
                            return;
                        }

                        int usage = ordinalUsages.get(ordinal);
                        if (usage == 0) {
                            return;
                        }

                        ordinalUsages.set(ordinal, --usage);
                        if (usage == 0) {
                            unusedOrdinals++;
                        }
                    }
                });
            }

            double unusedRatio = (unusedOrdinals / (double) parentValues.size());
            if (unusedRatio >= optimizeParentValuesRatio) {
                logger.info("Rebuilding parent ordinals on next refresh. Configured ratio [{}], actual ratio [{}]. Current parent ordinals size [{}] and unused ordinals [{}]", optimizeParentValuesRatio, unusedRatio, parentValues.size(), unusedOrdinals);
                rebuild = true;
            } else {
                logger.trace("NOT rebuilding parent ordinals. Configured ratio [{}], actual ratio [{}]. Current parent ordinals size [{}] and unused ordinals [{}]", optimizeParentValuesRatio, unusedRatio, parentValues.size(), unusedOrdinals);
            }
        }
    }

    public final static class Builder extends ParentOrdinals.Builder<ExpandableParentOrdinals> {

        private final double optimizeParentValuesRatio;

        @Inject
        public Builder(ShardId shardId, @IndexSettings Settings indexSettings) {
            super(shardId, indexSettings);
            this.optimizeParentValuesRatio = indexSettings.getAsDouble("index.rebuild_parent_values_ratio", 0.5);
        }

        @Override
        public ExpandableParentOrdinals doBuild(ExpandableParentOrdinals current, IndicesWarmer.WarmerContext warmerContext, NavigableSet<BytesRef> parentTypes) throws IOException {
            if (current == null) {
                current = new ExpandableParentOrdinals(logger, optimizeParentValuesRatio);
            }

            final IndexReader indexReader;
            if (current.rebuild) {
                if (warmerContext.completeSearcher() == null) {
                    return current; // Invoked during a merge, we can bail b/c the normal warming will also invoke us.
                }
                indexReader = warmerContext.completeSearcher().reader();
                current = new ExpandableParentOrdinals(logger, optimizeParentValuesRatio);
            } else {
                indexReader = warmerContext.newSearcher().reader();
            }

            synchronized (current.readerToTypes) {
                for (AtomicReaderContext context : indexReader.leaves()) {
                    AtomicReader reader = context.reader();
                    if (current.readerToTypes.containsKey(reader.getCoreCacheKey())) {
                        continue;
                    }

                    Map<String, Segment.Builder> typeToOrdinalsBuilder = new HashMap<String, Segment.Builder>();
                    MultiTermsEnum termsEnum = TermsEnums.getCompoundTermsEnum(reader, parentTypes, UidFieldMapper.NAME, ParentFieldMapper.NAME);
                    for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                        String type = ((TermsEnums.ParentUidTermsEnum) termsEnum.getMatchArray()[0].terms).type();
                        BytesRef id = ((TermsEnums.ParentUidTermsEnum) termsEnum.getMatchArray()[0].terms).id();
                        int ordinal = current.parentValues.add(id);
                        if (ordinal < 0) {
                            ordinal = -ordinal - 1;
                            int usage = current.ordinalUsages.get(ordinal);
                            if (usage == 0) {
                                current.unusedOrdinals--;
                            }
                            current.ordinalUsages.set(ordinal, ++usage);
                        } else {
                            current.ordinalUsages.add(1);
                        }

                        Segment.Builder builder = typeToOrdinalsBuilder.get(type);
                        if (builder == null) {
                            typeToOrdinalsBuilder.put(type, builder = new Segment.Builder(reader.maxDoc()));
                        }

                        DocsEnum docsEnum = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                        for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                            builder.set(docId, ordinal);
                        }
                    }

                    Map<String, Segment> typeToOrdinals = new HashMap<String, Segment>();
                    for (Map.Entry<String, Segment.Builder> entry : typeToOrdinalsBuilder.entrySet()) {
                        typeToOrdinals.put(entry.getKey(), entry.getValue().build());
                    }

                    if (reader instanceof SegmentReader) {
                        ((SegmentReader) reader).addCoreClosedListener(current);
                    }
                    current.readerToTypes.put(reader.getCoreCacheKey(), typeToOrdinals);
                }
            }

            return current;
        }

    }

}
