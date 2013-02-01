/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

import java.io.IOException;

/**
 */
public class LongValuesComparator extends FieldComparator<Long> {

    private final IndexNumericFieldData indexFieldData;
    final long missingValue;
    final boolean reversed;

    final long[] values;
    long bottom;

    public LongValuesComparator(IndexNumericFieldData indexFieldData, long missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.values = new long[numHits];
        this.reversed = reversed;
    }

    private LongValuesComparator(long[] values, long missingValue, boolean reversed, long bottom) {
        this.indexFieldData = null;
        this.missingValue = missingValue;
        this.values = values;
        this.reversed = reversed;
        this.bottom = bottom;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final long v1 = values[slot1];
        final long v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void setBottom(int slot) {
        this.bottom = values[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
        LongValues readerValues = indexFieldData.load(context).getLongValues();
        if (readerValues.isMultiValued()) {
            return new MV(readerValues);
        } else {
            return new SV(readerValues);
        }
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Long valueObj) throws IOException {
        throw new UnsupportedOperationException();
    }

    private abstract class PerSegmentComparator extends FieldComparator<Long> {

        protected final LongValues readerValues;

        protected PerSegmentComparator(LongValues readerValues) {
            this.readerValues = readerValues;
        }

        @Override
        public int compare(int slot1, int slot2) {
            return LongValuesComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(int slot) {
            LongValuesComparator.this.setBottom(slot);
        }

        @Override
        public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
            return LongValuesComparator.this.setNextReader(context);
        }

        @Override
        public Long value(int slot) {
            return LongValuesComparator.this.value(slot);
        }

    }

    private class SV extends PerSegmentComparator {

        private SV(LongValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            long v2 = readerValues.getValueMissing(doc, missingValue);

            if (bottom > v2) {
                return 1;
            } else if (bottom < v2) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = readerValues.getValueMissing(doc, missingValue);
        }

        @Override
        public int compareDocToValue(int doc, Long valueObj) throws IOException {
            final long value = valueObj.longValue();
            long docValue = readerValues.getValueMissing(doc, missingValue);
            if (docValue < value) {
                return -1;
            } else if (docValue > value) {
                return 1;
            } else {
                return 0;
            }
        }

    }

    private class MV extends PerSegmentComparator {

        private MV(LongValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            long v2 = getRelevantValue(readerValues, doc, missingValue, reversed);

            if (bottom > v2) {
                return 1;
            } else if (bottom < v2) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = getRelevantValue(readerValues, doc, missingValue, reversed);
        }

        @Override
        public int compareDocToValue(int doc, Long valueObj) throws IOException {
            final long value = valueObj.longValue(); // unbox once
            long docValue = getRelevantValue(readerValues, doc, missingValue, reversed);

            if (docValue < value) {
                return -1;
            } else if (docValue > value) {
                return 1;
            } else {
                return 0;
            }
        }

    }

    static long getRelevantValue(LongValues readerValues, int docId, long missing, boolean reversed) {
        LongValues.Iter iter = readerValues.getIter(docId);
        if (!iter.hasNext()) {
            return missing;
        }

        long currentVal = iter.next();
        long relevantVal = currentVal;
        while (true) {
            if (reversed) {
                if (currentVal > relevantVal) {
                    relevantVal = currentVal;
                }
            } else {
                if (currentVal < relevantVal) {
                    relevantVal = currentVal;
                }
            }
            if (!iter.hasNext()) {
                break;
            }
            currentVal = iter.next();
        }
        return relevantVal;
        // If we have a method on readerValues that tells if the values emitted by Iter or ArrayRef are sorted per
        // document that we can do this or something similar:
        // (This is already possible, if values are loaded from index, but we just need a method that tells us this
        // For example a impl that read values from the _source field might not read values in order)
        /*if (reversed) {
            // Would be nice if there is a way to get highest value from LongValues. The values are sorted anyway.
            LongArrayRef ref = readerValues.getValues(doc);
            if (ref.isEmpty()) {
                return missing;
            } else {
                return ref.values[ref.end - 1]; // last element is the highest value.
            }
        } else {
            return readerValues.getValueMissing(doc, missing); // returns lowest
        }*/
    }

}
