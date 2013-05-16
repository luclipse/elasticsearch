package org.elasticsearch.index.search.child.fieldcomparator.number;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.fieldcomparator.AbstractChildFieldComparator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.fieldcomparator.CompareUtil.compareLng;

/**
 */
public class LongFieldValuesCollector extends AbstractNaturalNumberFieldValuesCollector {

    private final TObjectLongMap<HashedBytesArray> childValues;
    private final long missingValue;

    public LongFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue, boolean reversed) {
        super(parentType, sortMode, context, indexFieldData);
        if (missingValue == null || "_last".equals(missingValue)) {
            this.missingValue = reversed ? Long.MIN_VALUE : Long.MAX_VALUE;
        } else if ("_first".equals(missingValue)) {
            this.missingValue = reversed ? Long.MAX_VALUE : Long.MIN_VALUE;
        } else {
            this.missingValue = missingValue instanceof Number ? ((Number) missingValue).longValue() : Long.parseLong(missingValue.toString());
        }
        this.childValues = new TObjectLongHashMap<HashedBytesArray>(
                Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, this.missingValue
        );
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        long newValue = longValues.getValueMissing(doc, missingValue);
        if (childValues.containsKey(parentId)) {
            long currentValue = childValues.get(parentId);
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

    private abstract class BaseLong extends AbstractChildFieldComparator<Long> {

        final long[] values;
        long bottom;

        protected BaseLong(SearchContext searchContext, int numHits) {
            super(parentType, searchContext);
            this.values = new long[numHits];
        }

        @Override
        public int compare(int slot1, int slot2) {
            return compareLng(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public Long value(int slot) {
            return values[slot];
        }

    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        if (sortMode == SortMode.AVG) {
            return new BaseLong(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareLng(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid) / count.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Long value) {
                    long val = value.longValue();
                    long docValue = childValues.get(uid) / count.get(uid);
                    return compareLng(docValue, val);
                }

            };
        } else {
            return new BaseLong(context, numHits) {

                @Override
                protected int compareBottom(HashedBytesArray uid) {
                    return compareLng(bottom, childValues.get(uid));
                }

                @Override
                protected void copy(int slot, HashedBytesArray uid) {
                    values[slot] = childValues.get(uid);
                }

                @Override
                protected int compareDocToValue(HashedBytesArray uid, Long value) {
                    long val = value.longValue();
                    long docValue = childValues.get(uid);
                    return compareLng(docValue, val);
                }
            };
        }
    }
}
