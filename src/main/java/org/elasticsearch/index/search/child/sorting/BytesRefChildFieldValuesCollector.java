package org.elasticsearch.index.search.child.sorting;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefValComparator;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.index.search.child.sorting.CompareUtil.compareBytesRef;

/**
 */
// TODO: Add BytesRefOrd based impl
public class BytesRefChildFieldValuesCollector extends AbstractChildFieldValuesCollector {

    private final IndexFieldData indexFieldData;
    private final ExtTHashMap<HashedBytesArray, BytesRef> childValues;
    private final SortMode sortMode;

    private BytesValues bytesValues;

    public BytesRefChildFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexFieldData indexFieldData) {
        super(parentType, context);
        this.sortMode = sortMode;
        this.indexFieldData = indexFieldData;
        this.childValues = CacheRecycler.popHashMap();
    }

    @Override
    public void collect(int doc, HashedBytesArray parentId) throws IOException {
        BytesRef currentVal = bytesValues.getValue(doc);
        BytesRef existingVal = childValues.get(parentId);
        if (existingVal != null) {
            int cmp = compareBytesRef(currentVal, existingVal);
            switch (sortMode) {
                case MIN:
                    if (cmp < 0) {
                        childValues.put(parentId, makeSafe(currentVal));
                    }
                    break;
                case MAX:
                    if (cmp > 0) {
                        childValues.put(parentId, makeSafe(currentVal));
                    }
                    break;
            }
        } else {
            childValues.put(parentId, makeSafe(currentVal));
        }
    }

    BytesRef makeSafe(BytesRef val) {
        if (val == null) {
            return null;
        }
        return bytesValues.makeSafe(val);
    }

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        super.setNextReader(readerContext);
        AtomicFieldData atomicFieldData = indexFieldData.load(readerContext);
        bytesValues = atomicFieldData.getBytesValues();
        if (atomicFieldData.isMultiValued()) {
            bytesValues = new BytesRefValComparator.MultiValuedBytesWrapper(bytesValues, sortMode);
        }
    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(final int numHits, int sortPos, boolean reversed) {
        return new AbstractChildFieldComparator<BytesRef>(parentType, context) {

            final BytesRef[] values = new BytesRef[numHits];
            BytesRef bottom;

            @Override
            protected int compareBottom(HashedBytesArray uid) {
                return compareBytesRef(bottom, childValues.get(uid));
            }

            @Override
            protected void copy(int slot, HashedBytesArray uid) {
                values[slot] = childValues.get(uid);
            }

            @Override
            protected int compareDocToValue(HashedBytesArray uid, BytesRef value) {
                return compareBytesRef(childValues.get(uid), value);
            }

            @Override
            public int compare(int slot1, int slot2) {
                return compareBytesRef(values[slot1], values[slot2]);
            }

            @Override
            public void setBottom(int slot) {
                bottom = values[slot];
            }

            @Override
            public BytesRef value(int slot) {
                return values[slot];
            }
        };
    }
}

/*private class ByteRefOrdChildFieldValuesCollector extends AbstractChildFieldValuesCollector {

        @Override
        public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
            return BytesRefOrdValComparator.this.setNextReader(context);
        }

        @Override
        public int compare(int slot1, int slot2) {
            return BytesRefOrdValComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(final int bottom) {
            BytesRefOrdValComparator.this.setBottom(bottom);
        }

        @Override
        public BytesRef value(int slot) {
            return BytesRefOrdValComparator.this.value(slot);
        }

        @Override
        public int compareValues(BytesRef val1, BytesRef val2) {
            if (val1 == null) {
                if (val2 == null) {
                    return 0;
                }
                return -1;
            } else if (val2 == null) {
                return 1;
            }
            return val1.compareTo(val2);
        }

        @Override
        public int compareDocToValue(int doc, BytesRef value) {
            return BytesRefOrdValComparator.this.compareDocToValue(doc, value);
        }
    }*/
