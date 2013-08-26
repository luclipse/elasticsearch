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
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.CompoundTermsEnum;
import org.elasticsearch.index.parentordinals.ParentOrdinalService;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

/**
 */
public class ExpandableParentOrdinalService extends ParentOrdinalService {

    // Can't hold more than 2G of parent ids..
    private final BytesRefHash parentValues;
    private final IntArrayList ordinalUsages;
    private final double optimizeParentValuesRatio;

    private volatile boolean optimizeParentValues;
    private volatile int unusedOrdinals;

    @Inject
    public ExpandableParentOrdinalService(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings, mapperService);
        this.parentValues = new BytesRefHash();
        this.parentValues.add(new BytesRef(BytesRef.EMPTY_BYTES));
        this.ordinalUsages = new IntArrayList();
        this.ordinalUsages.add(0);
        this.optimizeParentValuesRatio = componentSettings.getAsDouble("optimize_parent_values_ratio", 0.5);
    }

    protected void doRefresh(IndicesWarmer.WarmerContext warmerContext, NavigableSet<HashedBytesArray> parentTypes) throws IOException {
        // We don't need this lock, since we only refresh via warmer...
        synchronized (readerToTypes) {
            final IndexReader indexReader;
            if (optimizeParentValues) {
                if (warmerContext.completeSearcher() == null) {
                    return; // Invoked during a merge, we can bail b/c the normal warming will also invoke us.
                }
                indexReader = warmerContext.completeSearcher().reader();
                optimizeParentValues = false;

                // for now just clear it, but we can iterate over the existing used values and build a new br hash
                parentValues.clear(false);
                parentValues.reinit();
                parentValues.add(new BytesRef(BytesRef.EMPTY_BYTES));
                ordinalUsages.clear();
                ordinalUsages.add(0);
                readerToTypes.clear();
                unusedOrdinals = 0;
            } else {
                indexReader = warmerContext.newSearcher().reader();
            }

            for (AtomicReaderContext context : indexReader.leaves()) {
                AtomicReader reader = context.reader();
                if (readerToTypes.containsKey(reader.getCoreCacheKey())) {
                    continue;
                }

                Map<String, ParentOrdinals.Builder> typeToOrdinalsBuilder = new HashMap<String, ParentOrdinals.Builder>();
                TermsEnum termsEnum = CompoundTermsEnum.getCompoundTermsEnum(reader, UidFieldMapper.NAME, ParentFieldMapper.NAME);
                for (BytesRef term  = termsEnum.next(); term != null; term = termsEnum.next()) {
                    HashedBytesArray[] typeAndId = Uid.splitUidIntoTypeAndId(term);
                    if (!parentTypes.contains(typeAndId[0])) {
                        do {
                            HashedBytesArray nextParent = parentTypes.ceiling(typeAndId[0]);
                            if (nextParent == null) {
                                break;
                            }

                            TermsEnum.SeekStatus status = termsEnum.seekCeil(nextParent.toBytesRef());
                            if (status == TermsEnum.SeekStatus.END) {
                                break;
                            } else if (status == TermsEnum.SeekStatus.NOT_FOUND) {
                                term = termsEnum.term();
                                typeAndId = Uid.splitUidIntoTypeAndId(term);
                            } else if (status == TermsEnum.SeekStatus.FOUND) {
                                assert false : "Seek status should never be FOUND, because we seek only the type part";
                                term = termsEnum.term();
                                typeAndId = Uid.splitUidIntoTypeAndId(term);
                            }
                        } while (!parentTypes.contains(typeAndId[0]));
                    }
                    int ordinal = this.parentValues.add(typeAndId[1].toBytesRef());
                    if (ordinal < 0) {
                        ordinal = -ordinal - 1;
                        int usage = ordinalUsages.get(ordinal);
                        if (usage == 0) {
                            unusedOrdinals--;
                        }
                        ordinalUsages.set(ordinal, ++usage);
                    } else {
                        ordinalUsages.add(1);
                    }

                    ParentOrdinals.Builder builder = typeToOrdinalsBuilder.get(typeAndId[0].toUtf8());
                    if (builder == null) {
                        typeToOrdinalsBuilder.put(typeAndId[0].toUtf8(), builder = new ParentOrdinals.Builder(reader.maxDoc()));
                    }

                    DocsEnum docsEnum = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                    for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                        builder.set(docId, ordinal);
                    }
                }

                Map<String, ParentOrdinals> typeToOrdinals = new HashMap<String, ParentOrdinals>();
                for (Map.Entry<String, ParentOrdinals.Builder> entry : typeToOrdinalsBuilder.entrySet()) {
                    typeToOrdinals.put(entry.getKey(), entry.getValue().build());
                }

                if (reader instanceof SegmentReader) {
                    ((SegmentReader) reader).addCoreClosedListener(this);
                }
                readerToTypes.put(reader.getCoreCacheKey(), typeToOrdinals);
            }
        }
    }

    @Override
    public boolean supportsValueLookup() {
        return true;
    }

    @Override
    public void onClose(Object owner) {
        Map<String, ParentOrdinals> segmentValues = readerToTypes.remove(owner);
        if (segmentValues == null) {
            return;
        }

        synchronized (readerToTypes) {
            for (ParentOrdinals parentOrdinals : segmentValues.values()) {
                parentOrdinals.forEachOrdinal(new ParentOrdinals.OnOrdinal() {
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
                logger.info("[{}] Optimizing parent ordinals on next refresh. Configured ratio [{}], actual ratio [{}]. Current parent ordinals size [{}] and unused ordinals [{}]", index, optimizeParentValuesRatio, unusedRatio, parentValues.size(), unusedOrdinals);
                optimizeParentValues = true;
            } else {
                logger.trace("[{}] NOT optimizing parent ordinals. Configured ratio [{}], actual ratio [{}]. Current parent ordinals size [{}] and unused ordinals [{}]", index, optimizeParentValuesRatio, unusedRatio, parentValues.size(), unusedOrdinals);
            }
        }
    }

    public BytesRef parentValue(int ordinal, BytesRef spare) {
        return parentValues.get(ordinal, spare);
    }

}
