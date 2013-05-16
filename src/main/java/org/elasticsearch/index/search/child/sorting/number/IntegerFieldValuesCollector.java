package org.elasticsearch.index.search.child.sorting.number;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.sorting.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.sorting.CompareUtil.compareInt;

/**
 */
public class IntegerFieldValuesCollector extends AbstractNaturalNumberFieldValuesCollector {

    private final int missingValue;
    private final TObjectIntHashMap<HashedBytesArray> childValues;

    public IntegerFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (rawMissingValue == null || "_last".equals(rawMissingValue)) {
            this.missingValue = reversed ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        } else if ("_first".equals(rawMissingValue)) {
            this.missingValue = reversed ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        } else {
            this.missingValue = rawMissingValue instanceof Number ? ((Number) rawMissingValue).intValue() : Integer.parseInt(rawMissingValue.toString());
        }
        this.childValues = CacheRecycler.popObjectIntMap();
    }

    @Override
    public void clear() {
        CacheRecycler.pushObjectIntMap(childValues);
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        int newValue = (int) longValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            int currentValue = childValues.get(parentId);
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

    private abstract class BaseInt extends AbstractChildFieldComparator<Integer> {

        final int[] values;
        int bottom;

        protected BaseInt(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new int[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareInt(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Integer value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseInt(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareInt(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid) / count.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Integer value) {
                    int val = value.intValue();
                    int docValue = childValues.get(uid) / count.get(uid);
                    return compareInt(docValue, val);
                }

            };
        } else {
            return new BaseInt(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareInt(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Integer value) {
                    int val = value.intValue();
                    int docValue = childValues.get(uid);
                    return compareInt(docValue, val);
                }
            };
        }
    }
}
