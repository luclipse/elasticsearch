/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 * Sorts by field's natural Term sort order.  All
 * comparisons are done using BytesRef.compareTo, which is
 * slow for medium to large result sets but possibly
 * very fast for very small results sets.
 */
public final class BytesRefValComparator extends FieldComparator<BytesRef> {

    private final IndexFieldData indexFieldData;
    private final boolean reversed;

    private BytesRef[] values;
    private BytesRef bottom;

    BytesRefValComparator(IndexFieldData indexFieldData, int numHits, boolean reversed) {
        this.reversed = reversed;
        values = new BytesRef[numHits];
        this.indexFieldData = indexFieldData;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }

        return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        BytesValues docTerms = indexFieldData.load(context).getBytesValues();
        if (docTerms.isMultiValued()) {
            return new MV(docTerms);
        } else {
            return new SV(docTerms);
        }
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
        throw new UnsupportedOperationException();
    }

    private abstract class PerSegment extends FieldComparator<BytesRef> {

        protected final BytesValues docTerms;

        protected PerSegment(BytesValues docTerms) {
            this.docTerms = docTerms;
        }

        @Override
        public int compare(int slot1, int slot2) {
            return BytesRefValComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(int slot) {
            BytesRefValComparator.this.setBottom(slot);
        }

        @Override
        public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
            return BytesRefValComparator.this.setNextReader(context);
        }

        @Override
        public BytesRef value(int slot) {
            return BytesRefValComparator.this.value(slot);
        }
    }

    private class SV extends PerSegment {

        private SV(BytesValues docTerms) {
            super(docTerms);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            BytesRef val2 = docTerms.getValue(doc);
            if (bottom == null) {
                if (val2 == null) {
                    return 0;
                }
                return -1;
            } else if (val2 == null) {
                return 1;
            }
            return bottom.compareTo(val2);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            if (values[slot] == null) {
                values[slot] = new BytesRef();
            }
            docTerms.getValueScratch(doc, values[slot]);
        }

        @Override
        public int compareDocToValue(int doc, BytesRef value) throws IOException {
            return docTerms.getValue(doc).compareTo(value);
        }
    }

    private class MV extends PerSegment {

        private MV(BytesValues docTerms) {
            super(docTerms);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            BytesRef val2 = getRelevantValue(docTerms, doc, reversed);
            if (bottom == null) {
                if (val2 == null) {
                    return 0;
                }
                return -1;
            } else if (val2 == null) {
                return 1;
            }
            return bottom.compareTo(val2);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            if (values[slot] == null) {
                values[slot] = new BytesRef();
            }
            BytesRef value = getRelevantValue(docTerms, doc, reversed);
            values[slot].copyBytes(value);
        }

        @Override
        public int compareDocToValue(int doc, BytesRef value) throws IOException {
            BytesRef docValue = getRelevantValue(docTerms, doc, reversed);
            if (docValue == null) {
                if (value == null) {
                    return 0;
                }
                return -1;
            } else if (value == null) {
                return 1;
            }
            return docValue.compareTo(value);
        }

    }

    static BytesRef getRelevantValue(BytesValues readerValues, int docId, boolean reversed) {
        BytesValues.Iter iter = readerValues.getIter(docId);
        if (!iter.hasNext()) {
            return null;
        }

        BytesRef currentVal = iter.next();
        BytesRef relevantVal = currentVal;
        while (true) {
            int cmp = currentVal.compareTo(relevantVal);
            if (reversed) {
                if (cmp > 0) {
                    relevantVal = currentVal;
                }
            } else {
                if (cmp < 0) {
                    relevantVal = currentVal;
                }
            }
            if (!iter.hasNext()) {
                break;
            }
            currentVal = iter.next();
        }
        return relevantVal;
        /*if (reversed) {
            BytesRefArrayRef ref = readerValues.getValues(docId);
            if (ref.isEmpty()) {
                return null;
            } else {
                return ref.values[ref.end - 1]; // last element is the highest value.
            }
        } else {
            return readerValues.getValue(docId); // returns the lowest value
        }*/
    }

}
