package org.elasticsearch.index.search.child.sorting.number;

import gnu.trove.map.hash.TObjectFloatHashMap;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.sorting.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.sorting.CompareUtil.compareFloat;

/**
 */
public class FloatFieldValuesCollector extends AbstractFloatingNaturalNumberBase {

    private final TObjectFloatHashMap<HashedBytesArray> childValues;
    private final float missingValue;

    public FloatFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (missingValue == null || "_last".equals(missingValue)) {
            this.missingValue = reversed ? Float.MIN_VALUE : Float.MAX_VALUE;
        } else if ("_first".equals(missingValue)) {
            this.missingValue = reversed ? Float.MAX_VALUE : Float.MIN_VALUE;
        } else {
            this.missingValue = missingValue instanceof Number ? ((Number) missingValue).floatValue() : Float.parseFloat(missingValue.toString());
        }
        this.childValues = CacheRecycler.popObjectFloatMap();
    }

    @Override
    public void clear() {
        CacheRecycler.pushObjectFloatMap(childValues);
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        float newValue = (float) doubleValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            float currentValue = childValues.get(parentId);
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

    private abstract class BaseFloat extends AbstractChildFieldComparator<Float> {

        final float[] values;
        float bottom;

        protected BaseFloat(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new float[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareFloat(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Float value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseFloat(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareFloat(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid) / count.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Float value) {
                    float val = value.floatValue();
                    float docValue = childValues.get(uid) / count.get(uid);
                    return compareFloat(docValue, val);
                }

            };
        } else {
            return new BaseFloat(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareFloat(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Float value) {
                    float val = value.floatValue();
                    float docValue = childValues.get(uid);
                    return compareFloat(docValue, val);
                }
            };
        }
    }
}
