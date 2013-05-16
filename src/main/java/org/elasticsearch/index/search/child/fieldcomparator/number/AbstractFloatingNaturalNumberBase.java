package org.elasticsearch.index.search.child.fieldcomparator.number;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorBase;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.fieldcomparator.AbstractChildFieldValuesCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public abstract class AbstractFloatingNaturalNumberBase extends AbstractChildFieldValuesCollector {

    private final IndexNumericFieldData indexFieldData;
    protected final TObjectIntHashMap<HashedBytesArray> count;
    protected final SortMode sortMode;

    DoubleValues doubleValues;

    AbstractFloatingNaturalNumberBase(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData) {
        super(parentType, context);
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        this.count = sortMode == SortMode.AVG ? CacheRecycler.<HashedBytesArray>popObjectIntMap() : null;
    }

    @Override
    public void clear() {
        if (sortMode == SortMode.AVG) {
            CacheRecycler.pushObjectIntMap(count);
        }
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
