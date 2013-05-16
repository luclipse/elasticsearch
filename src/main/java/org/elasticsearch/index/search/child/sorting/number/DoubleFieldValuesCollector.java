package org.elasticsearch.index.search.child.sorting.number;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.sorting.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.sorting.CompareUtil.compareDouble;

/**
 */
public class DoubleFieldValuesCollector extends AbstractFloatingNaturalNumberBase {

    private final TObjectDoubleMap<HashedBytesArray> childValues;
    private final double missingValue;

    public DoubleFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (missingValue == null || "_last".equals(missingValue)) {
            this.missingValue = reversed ? Double.MIN_VALUE : Double.MAX_VALUE;
        } else if ("_first".equals(missingValue)) {
            this.missingValue = reversed ? Double.MAX_VALUE : Double.MIN_VALUE;
        } else {
            this.missingValue = missingValue instanceof Number ? ((Number) missingValue).doubleValue() : Double.parseDouble(missingValue.toString());
        }
        this.childValues = new TObjectDoubleHashMap<HashedBytesArray>(
                Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, this.missingValue
        );
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        double newValue = doubleValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            double currentValue = childValues.get(parentId);
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

    private abstract class BaseDouble extends AbstractChildFieldComparator<Double> {

        final double[] values;
        double bottom;

        protected BaseDouble(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new double[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareDouble(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Double value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseDouble(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareDouble(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid) / count.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Double value) {
                    double val = value.doubleValue();
                    double docValue = childValues.get(uid) / count.get(uid);
                    return compareDouble(docValue, val);
                }

            };
        } else {
            return new BaseDouble(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareDouble(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Double value) {
                    double val = value.doubleValue();
                    double docValue = childValues.get(uid);
                    return compareDouble(docValue, val);
                }
            };
        }
    }
}
