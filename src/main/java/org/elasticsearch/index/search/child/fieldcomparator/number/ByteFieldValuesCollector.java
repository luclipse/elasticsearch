package org.elasticsearch.index.search.child.fieldcomparator.number;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectByteMap;
import gnu.trove.map.hash.TObjectByteHashMap;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.fieldcomparator.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.fieldcomparator.CompareUtil.compareByte;

/**
 */
public class ByteFieldValuesCollector extends AbstractNaturalNumberFieldValuesCollector {

    private final byte missingValue;
    private final TObjectByteMap<HashedBytesArray> childValues;

    public ByteFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (rawMissingValue == null || "_last".equals(rawMissingValue)) {
            this.missingValue = reversed ? Byte.MIN_VALUE : Byte.MAX_VALUE;
        } else if ("_first".equals(rawMissingValue)) {
            this.missingValue = reversed ? Byte.MAX_VALUE : Byte.MIN_VALUE;
        } else {
            this.missingValue = rawMissingValue instanceof Number ? ((Number) rawMissingValue).byteValue() : Byte.parseByte(rawMissingValue.toString());
        }
        this.childValues = new TObjectByteHashMap<HashedBytesArray>(
                Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, missingValue
        );
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        byte newValue = (byte) longValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            byte currentValue = childValues.get(parentId);
            switch (sortMode) {
                case MIN:
                    if (newValue < currentValue) {
                        currentValue = newValue;
                    }
                    break;
                case MAX:
                    if (newValue > currentValue) {
                        currentValue = newValue;
                    }
                    break;
                case SUM:
                    currentValue += newValue;
                    break;
                case AVG:
                    currentValue += newValue;
                    count.increment(parentId);
                    break;
            }
            childValues.put(parentId, currentValue);
        } else {
            childValues.put(parentId, newValue);
            if (sortMode == SortMode.AVG) {
                count.put(parentId, 1);
            }
        }
    }

    private abstract class BaseByte extends AbstractChildFieldComparator<Byte> {

        final byte[] values;
        byte bottom;

        protected BaseByte(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new byte[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareByte(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Byte value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseByte(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareByte(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = (byte) (childValues.get(uid) / count.get(uid));
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Byte value) {
                    byte val = value.byteValue();
                    byte docValue = (byte) (childValues.get(uid) / count.get(uid));
                    return compareByte(docValue, val);
                }

            };
        } else {
            return new BaseByte(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareByte(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Byte value) {
                    byte val = value.byteValue();
                    byte docValue = childValues.get(uid);
                    return compareByte(docValue, val);
                }
            };
        }
    }
}
