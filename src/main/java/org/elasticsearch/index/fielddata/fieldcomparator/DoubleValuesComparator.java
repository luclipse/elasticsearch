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
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

import java.io.IOException;

/**
 */
public class DoubleValuesComparator extends FieldComparator<Double> {

    private final IndexNumericFieldData indexFieldData;
    private final double missingValue;
    private final boolean reversed;

    protected final double[] values;
    private double bottom;

    public DoubleValuesComparator(IndexNumericFieldData indexFieldData, double missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
        this.values = new double[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final double v1 = values[slot1];
        final double v2 = values[slot2];
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
    public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
        DoubleValues readerValues = indexFieldData.load(context).getDoubleValues();
        if (readerValues.isMultiValued()) {
            return new MV(readerValues);
        } else {
            return new SV(readerValues);
        }
    }

    @Override
    public Double value(int slot) {
        return Double.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Double valueObj) throws IOException {
        throw new UnsupportedOperationException();
    }

    private abstract class PerSegment extends FieldComparator<Double> {

        protected final DoubleValues readerValues;

        protected PerSegment(DoubleValues readerValues) {
            this.readerValues = readerValues;
        }

        @Override
        public int compare(int slot1, int slot2) {
            return DoubleValuesComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(int slot) {
            DoubleValuesComparator.this.setBottom(slot);
        }

        @Override
        public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
            return DoubleValuesComparator.this.setNextReader(context);
        }

        @Override
        public Double value(int slot) {
            return DoubleValuesComparator.this.value(slot);
        }
    }

    private class SV extends PerSegment {

        private SV(DoubleValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            double v2 = readerValues.getValueMissing(doc, missingValue);

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
        public int compareDocToValue(int doc, Double valueObj) throws IOException {
            final double value = valueObj.doubleValue();
            double docValue = readerValues.getValueMissing(doc, missingValue);
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

        private MV(DoubleValues readerValues) {
            super(readerValues);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            double v2 = getRelevantValue(readerValues, doc, missingValue, reversed);

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
        public int compareDocToValue(int doc, Double valueObj) throws IOException {
            final double value = valueObj.doubleValue();
            double docValue = getRelevantValue(readerValues, doc, missingValue, reversed);
            if (docValue < value) {
                return -1;
            } else if (docValue > value) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    static double getRelevantValue(DoubleValues readerValues, int docId, double missing, boolean reversed) {
        DoubleValues.Iter iter = readerValues.getIter(docId);
        if (!iter.hasNext()) {
            return missing;
        }

        double currentVal = iter.next();
        double relevantVal = currentVal;
        while (true) {
            int cmp = Double.compare(currentVal, relevantVal);
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
    }

}
