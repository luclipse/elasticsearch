package org.elasticsearch.index.search.child;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectByteMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.TObjectShortMap;
import gnu.trove.map.hash.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.search.nested.NestedFieldComparatorSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public class ChildFieldComparatorSource extends IndexFieldData.XFieldComparatorSource implements SearchContext.Rewrite {

    private final String parentType;
    private final Filter childFilter;
    private final SortMode sortMode;
    private final FieldMapper sortField;
    private final Object missingValue;
    private final boolean reversed;
    private final TObjectIntHashMap<HashedBytesArray> count;

    private IndexFieldData.XFieldComparatorSource innerFieldComparatorSource;
    private AbstractChildFieldValuesCollector collector;

    public ChildFieldComparatorSource(String parentType, Filter childFilter, SortMode sortMode, FieldMapper sortField, Object missingValue, boolean reversed) {
        this.parentType = parentType;
        this.childFilter = childFilter;
        this.sortMode = sortMode;
        this.sortField = sortField;
        this.missingValue = missingValue;
        this.reversed = reversed;
        if (sortMode == SortMode.AVG) {
            this.count = CacheRecycler.popObjectIntMap();
        } else {
            this.count = null;
        }
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        IndexFieldData indexFieldData = searchContext.fieldData().getForField(sortField);
        innerFieldComparatorSource = indexFieldData.comparatorSource(missingValue, sortMode);
        switch (innerFieldComparatorSource.reducedType()) {
            case BYTE:
                collector = new ByteVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                break;
            case SHORT:
                collector = new ShortVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                break;
            case INT:
                collector = new IntegerVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                break;
            case LONG:
                collector = new IntegerVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
//                collector = new LongVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                break;
            case FLOAT:
                collector = new FloatVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                break;
            case DOUBLE:
                if (innerFieldComparatorSource instanceof GeoDistanceComparatorSource) {
                    assert false; // TODO: add geo_distance support
                } else {
                    collector = new DoubleVal(parentType, searchContext, (IndexNumericFieldData) indexFieldData, missingValue);
                }
                break;
            case STRING:
                // TODO: add BytesRefOrdVal based impl
                if (indexFieldData.valuesOrdered() && indexFieldData instanceof IndexFieldData.WithOrdinals) {
                } else {
                }
                collector = new BytesRefChildFieldValuesCollector(parentType, searchContext, indexFieldData);
                break;
            default:
                assert false : "Are we missing a sort field type here? -- " + innerFieldComparatorSource.reducedType();
                break;
        }

        // If the child is inside one or more nested objects then wrap it.
        if (innerFieldComparatorSource instanceof NestedFieldComparatorSource) {
            NestedFieldComparatorSource nestedSource = (NestedFieldComparatorSource) innerFieldComparatorSource;
            collector = new NestedChildFieldValuesCollector(
                parentType, searchContext, collector, nestedSource.rootDocumentsFilter(), nestedSource.innerDocumentsFilter()
            );
        }
        searchContext.searcher().search(new XFilteredQuery(new MatchAllDocsQuery(), childFilter), collector);
    }

    @Override
    public void contextClear() {
        collector.clear();
        collector = null;
        if (sortMode == SortMode.AVG) {
            CacheRecycler.pushObjectIntMap(count);
        }
    }

    @Override
    public SortField.Type reducedType() {
        return innerFieldComparatorSource.reducedType();
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        return collector.getFieldComparator(numHits, sortPos, reversed);
    }

    private abstract class AbstractChildFieldComparator<T> extends FieldComparator<T> {

        private final SearchContext searchContext;
        private IdReaderTypeCache idCache;

        protected AbstractChildFieldComparator(SearchContext searchContext) {
            this.searchContext = searchContext;
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            HashedBytesArray uid = idCache.idByDoc(doc);
            return compareBottom(uid);
        }

        protected abstract int compareBottom(HashedBytesArray uid);

        @Override
        public void copy(int slot, int doc) throws IOException {
            HashedBytesArray uid = idCache.idByDoc(doc);
            copy(slot, uid);
        }

        protected abstract void copy(int slot, HashedBytesArray uid);

        @Override
        public FieldComparator<T> setNextReader(AtomicReaderContext readerContext) throws IOException {
            idCache = searchContext.idCache().reader(readerContext.reader()).type(parentType);
            return this;
        }

        @Override
        public int compareDocToValue(int doc, T value) throws IOException {
            HashedBytesArray uid = idCache.idByDoc(doc);
            return compareDocToValue(uid, value);
        }

        protected abstract int compareDocToValue(HashedBytesArray uid, T value);
    }

    private abstract class AbstractChildFieldValuesCollector extends ParentIdCollector {

        AbstractChildFieldValuesCollector(String parentType, SearchContext context) {
            super(parentType, context);
        }

        public abstract AbstractChildFieldComparator getFieldComparator(int numHits, int sortPos, boolean reversed);

        public void clear() {

        }
    }

    private class NestedChildFieldValuesCollector extends AbstractChildFieldValuesCollector {

        private final AbstractChildFieldValuesCollector wrapped;
        private final Filter rootDocumentsFilter;
        private final Filter innerDocumentsFilter;

        private FixedBitSet rootDocuments;
        private FixedBitSet innerDocuments;

        private NestedChildFieldValuesCollector(String parentType, SearchContext context, AbstractChildFieldValuesCollector wrapped, Filter rootDocumentsFilter, Filter innerDocumentsFilter) {
            super(parentType, context);
            this.wrapped = wrapped;
            this.rootDocumentsFilter = rootDocumentsFilter;
            this.innerDocumentsFilter = innerDocumentsFilter;
        }

        @Override
        public AbstractChildFieldComparator getFieldComparator(int numHits, int sortPos, boolean reversed) {
            return wrapped.getFieldComparator(numHits, sortPos, reversed);
        }

        @Override
        protected void collect(int rootDoc, HashedBytesArray parentId) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return;
            }

            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return;
            }
            do {
                wrapped.collect(nestedDoc, parentId);
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
            } while (nestedDoc >= rootDoc || nestedDoc == -1);
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            super.setNextReader(readerContext);
            DocIdSet innerDocuments = innerDocumentsFilter.getDocIdSet(readerContext, null);
            if (DocIdSets.isEmpty(innerDocuments)) {
                this.innerDocuments = null;
            } else if (innerDocuments instanceof FixedBitSet) {
                this.innerDocuments = (FixedBitSet) innerDocuments;
            } else {
                this.innerDocuments = DocIdSets.toFixedBitSet(innerDocuments.iterator(), readerContext.reader().maxDoc());
            }
            DocIdSet rootDocuments = rootDocumentsFilter.getDocIdSet(readerContext, null);
            if (DocIdSets.isEmpty(rootDocuments)) {
                this.rootDocuments = null;
            } else if (rootDocuments instanceof FixedBitSet) {
                this.rootDocuments = (FixedBitSet) rootDocuments;
            } else {
                this.rootDocuments = DocIdSets.toFixedBitSet(rootDocuments.iterator(), readerContext.reader().maxDoc());
            }
            wrapped.setNextReader(readerContext);
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

    // TODO: Add BytesRefOrd based impl
    private class BytesRefChildFieldValuesCollector extends AbstractChildFieldValuesCollector {

        private final IndexFieldData indexFieldData;
        private final ExtTHashMap<HashedBytesArray, BytesRef> childValues;

        private BytesValues bytesValues;

        private BytesRefChildFieldValuesCollector(String parentType, SearchContext context, IndexFieldData indexFieldData) {
            super(parentType, context);
            this.indexFieldData = indexFieldData;
            this.childValues = CacheRecycler.popHashMap();
        }

        @Override
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
            return new AbstractChildFieldComparator<BytesRef>(context) {

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

    private abstract class NaturalNumberBase extends AbstractChildFieldValuesCollector {

        private final IndexNumericFieldData indexFieldData;
        LongValues longValues;

        NaturalNumberBase(String parentType, SearchContext context, IndexNumericFieldData indexFieldData) {
            super(parentType, context);
            this.indexFieldData = indexFieldData;
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            super.setNextReader(readerContext);
            longValues = indexFieldData.load(readerContext).getLongValues();
            if (longValues.isMultiValued()) {
                longValues = new LongValuesComparatorBase.MultiValueWrapper(longValues, sortMode);
            }
        }

    }

    private class ByteVal extends NaturalNumberBase {

        private final byte missingValue;
        private final TObjectByteMap<HashedBytesArray> childValues;

        private ByteVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    private class ShortVal extends NaturalNumberBase {

        private final short missingValue;
        private final TObjectShortMap<HashedBytesArray> childValues;

        private ShortVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    private class IntegerVal extends NaturalNumberBase {

        private final int missingValue;
        private final TObjectIntHashMap<HashedBytesArray> childValues;

        private IntegerVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object rawMissingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    private class LongVal extends NaturalNumberBase {

        private final TObjectLongMap<HashedBytesArray> childValues;
        private final long missingValue;

        LongVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    private abstract class FloatingNaturalNumberBase extends AbstractChildFieldValuesCollector {

        private final IndexNumericFieldData indexFieldData;
        DoubleValues doubleValues;

        FloatingNaturalNumberBase(String parentType, SearchContext context, IndexNumericFieldData indexFieldData) {
            super(parentType, context);
            this.indexFieldData = indexFieldData;
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            super.setNextReader(readerContext);
            doubleValues = indexFieldData.load(readerContext).getDoubleValues();
            if (doubleValues.isMultiValued()) {
                doubleValues = new DoubleValuesComparatorBase.MultiValueWrapper(doubleValues, sortMode);
            }
        }

    }

    private class FloatVal extends FloatingNaturalNumberBase {

        private final TObjectFloatHashMap<HashedBytesArray> childValues;
        private final float missingValue;

        FloatVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    private class DoubleVal extends FloatingNaturalNumberBase {

        private final TObjectDoubleMap<HashedBytesArray> childValues;
        private final double missingValue;

        DoubleVal(String parentType, SearchContext context, IndexNumericFieldData indexFieldData, Object missingValue) {
            super(parentType, context, indexFieldData);
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
        protected void collect(int doc, HashedBytesArray parentId) throws IOException {
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
                super(searchContext);
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

    // Move to CompareUtil

    private static final int compareByte(byte left, byte right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareShort(short left, short right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareInt(int left, int right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareLng(long left, long right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareFloat(float left, float right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareDouble(double left, double right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    private static final int compareBytesRef(BytesRef val1, BytesRef val2) {
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

}
