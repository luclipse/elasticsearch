package org.elasticsearch.index.search.child.sorting.number;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectShortMap;
import gnu.trove.map.hash.TObjectShortHashMap;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.sorting.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.sorting.CompareUtil.compareShort;

/**
 */
public class ShortFieldValuesCollector extends AbstractNaturalNumberFieldValuesCollector {

    private final short missingValue;
    private final TObjectShortMap<HashedBytesArray> childValues;

    public ShortFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (rawMissingValue == null || "_last".equals(rawMissingValue)) {
            this.missingValue = reversed ? Short.MIN_VALUE : Short.MAX_VALUE;
        } else if ("_first".equals(rawMissingValue)) {
            this.missingValue = reversed ? Short.MAX_VALUE : Short.MIN_VALUE;
        } else {
            this.missingValue = rawMissingValue instanceof Number ? ((Number) rawMissingValue).shortValue() : Short.parseShort(rawMissingValue.toString());
        }
        this.childValues = new TObjectShortHashMap<HashedBytesArray>(
                Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, missingValue
        );
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        short newValue = (short) longValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            short currentValue = childValues.get(parentId);
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

    private abstract class BaseShort extends AbstractChildFieldComparator<Short> {

        final short[] values;
        short bottom;

        protected BaseShort(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new short[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareShort(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Short value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseShort(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareShort(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = (short) (childValues.get(uid) / count.get(uid));
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Short value) {
                    short val = value.shortValue();
                    short docValue = (short) (childValues.get(uid) / count.get(uid));
                    return compareShort(docValue, val);
                }

            };
        } else {
            return new BaseShort(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareShort(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Short value) {
                    short val = value.shortValue();
                    short docValue = childValues.get(uid);
                    return compareShort(docValue, val);
                }
            };
        }
    }
}
