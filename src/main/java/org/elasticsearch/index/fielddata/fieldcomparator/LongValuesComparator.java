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
    private final long missingValue;
    private final boolean reversed;
    private final LongValues readerValues;

    private final long[] values;
    private long bottom;

    public LongValuesComparator(IndexNumericFieldData indexFieldData, long missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.values = new long[numHits];
        this.reversed = reversed;
        this.readerValues = null;
    }

    private LongValuesComparator(LongValuesComparator previous, LongValues readerValues) {
        this.indexFieldData = previous.indexFieldData;
        this.missingValue = previous.missingValue;
        this.values = previous.values;
        this.reversed = previous.reversed;
        this.bottom = previous.bottom;
        this.readerValues = readerValues;
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
        long v2;
        if (reversed) {
            v2 = readerValues.getMaxValueMissing(doc, missingValue);
        } else {
            v2 = readerValues.getMinValueMissing(doc, missingValue);
        }

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
        if (reversed) {
            values[slot] = readerValues.getMaxValueMissing(doc, missingValue);
        } else {
            values[slot] = readerValues.getMinValueMissing(doc, missingValue);
        }
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
        LongValues readerValues = indexFieldData.load(context).getLongValues();
        return new LongValuesComparator(this, readerValues);
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Long valueObj) throws IOException {
        final long value = valueObj.longValue(); // unbox once
        long docValue;
        if (reversed) {
            docValue = readerValues.getMaxValueMissing(doc, missingValue);
        } else {
            docValue = readerValues.getMinValueMissing(doc, missingValue);
        }

        if (docValue < value) {
            return -1;
        } else if (docValue > value) {
            return 1;
        } else {
            return 0;
        }
    }

}
