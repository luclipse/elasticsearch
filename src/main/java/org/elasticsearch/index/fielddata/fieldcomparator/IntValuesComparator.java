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
import org.elasticsearch.index.fielddata.IntValues;

import java.io.IOException;

/**
 */
public class IntValuesComparator extends FieldComparator<Integer> {

    private final IndexNumericFieldData indexFieldData;
    private final int missingValue;
    private final boolean reversed;

    private final int[] values;
    private int bottom;

    public IntValuesComparator(IndexNumericFieldData indexFieldData, int missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
        this.values = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final int v1 = values[slot1];
        final int v2 = values[slot2];
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
    public FieldComparator<Integer> setNextReader(AtomicReaderContext context) throws IOException {
        IntValues readerValues = indexFieldData.load(context).getIntValues();
        if (readerValues.isMultiValued()) {
            return new MV(readerValues);
        } else {
            return new SV(readerValues);
        }
    }

    @Override
    public Integer value(int slot) {
        return Integer.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Integer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    private abstract class PerSegment extends FieldComparator<Integer> {

        protected final IntValues readerValues;

        protected PerSegment(IntValues readerValues) {
            this.readerValues = readerValues;
        }

        @Override
        public int compare(int slot1, int slot2) {
            return IntValuesComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(int slot) {
            IntValuesComparator.this.setBottom(slot);
        }

        @Override
        public FieldComparator<Integer> setNextReader(AtomicReaderContext context) throws IOException {
            return IntValuesComparator.this.setNextReader(context);
        }

        @Override
        public Integer value(int slot) {
            return IntValuesComparator.this.value(slot);
        }
    }

    private class SV extends PerSegment {

        private SV(IntValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            int v2 = readerValues.getValueMissing(doc, missingValue);

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
        public int compareDocToValue(int doc, Integer valueObj) throws IOException {
            final int value = valueObj.intValue();
            int docValue = readerValues.getValueMissing(doc, missingValue);
            if (docValue < value) {
                return -1;
            } else if (docValue > value) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private class MV extends PerSegment {

        private MV(IntValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            int v2 = getRelevantValue(readerValues, doc, missingValue, reversed);

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
        public int compareDocToValue(int doc, Integer valueObj) throws IOException {
            final int value = valueObj.intValue();
            int docValue = getRelevantValue(readerValues, doc, missingValue, reversed);
            if (docValue < value) {
                return -1;
            } else if (docValue > value) {
                return 1;
            } else {
                return 0;
            }
        }

    }

    static int getRelevantValue(IntValues readerValues, int docId, int missing, boolean reversed) {
        IntValues.Iter iter = readerValues.getIter(docId);
        if (!iter.hasNext()) {
            return missing;
        }

        int currentVal = iter.next();
        int relevantVal = currentVal;
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
    }

}
