package org.elasticsearch.index.search.child.sorting.number;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorBase;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.search.child.sorting.AbstractChildFieldValuesCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public abstract class AbstractNaturalNumberFieldValuesCollector extends AbstractChildFieldValuesCollector {

    private final IndexNumericFieldData indexFieldData;
    protected final TObjectIntHashMap<HashedBytesArray> count;
    protected final SortMode sortMode;

    protected LongValues longValues;

    protected AbstractNaturalNumberFieldValuesCollector(String parentType, SortMode sortMode, SearchContext context, IndexNumericFieldData indexFieldData) {
        super(parentType, context);
        this.sortMode = sortMode;
        this.indexFieldData = indexFieldData;
        if (sortMode == SortMode.AVG) {
            count = CacheRecycler.popObjectIntMap();
        } else {
            count = null;
        }
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
        longValues = indexFieldData.load(readerContext).getLongValues();
        if (longValues.isMultiValued()) {
            longValues = new LongValuesComparatorBase.MultiValueWrapper(longValues, sortMode);
        }
    }

}