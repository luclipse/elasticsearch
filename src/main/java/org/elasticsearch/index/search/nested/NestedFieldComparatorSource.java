package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.LongValues;

import java.io.IOException;

/**
 */
public class NestedFieldComparatorSource extends FieldComparatorSource {

    private final FieldComparatorSource wrappedSource;
    private final NestedOrder nestedOrder;
    private final Filter parentFilter;
    private final Filter childFilter;

    public NestedFieldComparatorSource(FieldComparatorSource wrappedSource, NestedOrder nestedOrder, Filter parentFilter, Filter childFilter) {
        this.wrappedSource = wrappedSource;
        this.nestedOrder = nestedOrder;
        this.parentFilter = parentFilter;
        this.childFilter = childFilter;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        FieldComparator wrappedComparator = wrappedSource.newComparator(fieldname, numHits, sortPos, reversed);
        switch (nestedOrder) {
            case FIRST:
                return new NestedFieldComparator.First(wrappedComparator, parentFilter, childFilter);
            case LAST:
                return new NestedFieldComparator.Last(wrappedComparator, parentFilter, childFilter);
            default:
                throw new ElasticSearchIllegalArgumentException("Illegal nested_order [" + nestedOrder + "]");
        }
    }

    public enum NestedOrder {

        /**
         * Sort based on the first encountered nested value.
         */
        FIRST,

        /**
         * Sort based on the last encountered nested value.
         */
        LAST,

        /**
         * Sort based on the sum of all nested values.
         */
        SUM;

        // TODO Add: middle value
        // TODO Add: avg, sum, max, min, but only for numerical fields

        public static NestedOrder parse(String type) {
            if ("first".equals(type)) {
                return FIRST;
            } else if ("last".equals(type)) {
                return LAST;
            } else if ("sum".equals(type)) {
                return SUM;
            }

            throw new ElasticSearchIllegalArgumentException("No nested sort type [" + type + "] found");
        }

    }
}

abstract class NestedFieldComparator extends FieldComparator {

    FieldComparator wrappedComparator;
    final Filter parentFilter;
    final Filter childFilter;

    FixedBitSet rootDocuments;
    FixedBitSet innerDocuments;

    NestedFieldComparator(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter) {
        this.wrappedComparator = wrappedComparator;
        this.parentFilter = parentFilter;
        this.childFilter = childFilter;
    }

    @Override
    public int compare(int slot1, int slot2) {
        return wrappedComparator.compare(slot1, slot2);
    }

    @Override
    public void setBottom(int slot) {
        wrappedComparator.setBottom(slot);
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
        wrappedComparator = wrappedComparator.setNextReader(context);
        rootDocuments = (FixedBitSet) parentFilter.getDocIdSet(context, null);
        innerDocuments = (FixedBitSet) childFilter.getDocIdSet(context, null);
        return this;
    }

    @Override
    public Object value(int slot) {
        return wrappedComparator.value(slot);
    }

    static class First extends NestedFieldComparator {

        First(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter) {
            super(wrappedComparator, parentFilter, childFilter);
        }

        @Override
        public int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return 0;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = prevRootDoc + 1; i < rootDoc; i++) {
                if (innerDocuments.get(i)) {
                    return wrappedComparator.compareBottom(i);
                }
            }
            return 0;
        }

        @Override
        public void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = prevRootDoc + 1; i < rootDoc; i++) {
                if (innerDocuments.get(i)) {
                    wrappedComparator.copy(slot, i);
                    return;
                }
            }
        }

        @Override
        public int compareDocToValue(int rootDoc, Object value) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return 0;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = prevRootDoc + 1; i < rootDoc; i++) {
                if (innerDocuments.get(i)) {
                    return wrappedComparator.compareDocToValue(i, value);
                }
            }

            return 0;
        }

    }

    static class Last extends NestedFieldComparator {

        Last(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter) {
            super(wrappedComparator, parentFilter, childFilter);
        }

        @Override
        public int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return 0;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = rootDoc - 1; i > prevRootDoc; i--) {
                if (innerDocuments.get(i)) {
                    return wrappedComparator.compareBottom(i);
                }
            }
            return 0;
        }

        @Override
        public void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = rootDoc - 1; i > prevRootDoc; i--) {
                if (innerDocuments.get(i)) {
                    wrappedComparator.copy(slot, i);
                    return;
                }
            }
        }

        @Override
        public int compareDocToValue(int rootDoc, Object value) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return 0;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = rootDoc - 1; i > prevRootDoc; i--) {
                if (innerDocuments.get(i)) {
                    return wrappedComparator.compareDocToValue(i, value);
                }
            }

            return 0;
        }

    }

    static class Sum extends NestedFieldComparator {

        LongValues longFieldData;
        long currentValue;

        LongValues.ValueInDocProc longValueInDocProc = new LongValues.ValueInDocProc() {

            @Override
            public void onValue(int docId, long value) {
                currentValue += value;
            }

            @Override
            public void onMissing(int docId) {
            }

        };

        Sum(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter) {
            super(wrappedComparator, parentFilter, childFilter);
        }

        @Override
        public int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return 0;
            }
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = prevRootDoc + 1; i < rootDoc; i++) {
                if (innerDocuments.get(i)) {
                    currentValue = 0;
                    longFieldData.forEachValueInDoc(i, longValueInDocProc);

                    return wrappedComparator.compareBottom(i);
                }
            }
            return 0;
        }

        @Override
        public void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null) {
                return;
            }

            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            for (int i = prevRootDoc + 1; i < rootDoc; i++) {
                if (innerDocuments.get(i)) {
                    wrappedComparator.copy(slot, i);
                    return;
                }
            }
        }

        @Override
        public int compareDocToValue(int doc, Object value) throws IOException {
            return 0;
        }
    }

}