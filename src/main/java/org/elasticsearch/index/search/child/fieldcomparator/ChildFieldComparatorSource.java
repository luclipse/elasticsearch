package org.elasticsearch.index.search.child.fieldcomparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.GeoDistanceComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.search.child.fieldcomparator.number.DoubleFieldValuesCollector;
import org.elasticsearch.index.search.child.fieldcomparator.number.LongFieldValuesCollector;
import org.elasticsearch.index.search.child.fieldcomparator.other.NestedChildFieldValuesCollector;
import org.elasticsearch.index.search.child.fieldcomparator.string.BytesRefChildFieldValuesCollector;
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

    private IndexFieldData.XFieldComparatorSource innerFieldComparatorSource;
    private AbstractChildFieldValuesCollector collector;

    public ChildFieldComparatorSource(String parentType, Filter childFilter, SortMode sortMode, FieldMapper sortField, Object missingValue, boolean reversed) {
        this.parentType = parentType;
        this.childFilter = childFilter;
        this.sortMode = sortMode;
        this.sortField = sortField;
        this.missingValue = missingValue;
        this.reversed = reversed;
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        IndexFieldData indexFieldData = searchContext.fieldData().getForField(sortField);
        innerFieldComparatorSource = indexFieldData.comparatorSource(missingValue, sortMode);
        switch (innerFieldComparatorSource.reducedType()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                collector = new LongFieldValuesCollector(
                    parentType, sortMode, searchContext, (IndexNumericFieldData) indexFieldData, missingValue, reversed
                );
                break;
            case FLOAT:
            case DOUBLE:
                if (innerFieldComparatorSource instanceof GeoDistanceComparatorSource) {
                    assert false; // TODO: add geo_distance support
                } else {
                    collector = new DoubleFieldValuesCollector(
                        parentType, sortMode, searchContext, (IndexNumericFieldData) indexFieldData, missingValue, reversed
                    );
                }
                break;
            case STRING:
                // TODO: add BytesRefOrdVal based impl
                if (indexFieldData.valuesOrdered() && indexFieldData instanceof IndexFieldData.WithOrdinals) {
                } else {
                }
                collector = new BytesRefChildFieldValuesCollector(parentType, sortMode, searchContext, indexFieldData);
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
    }

    @Override
    public SortField.Type reducedType() {
        return innerFieldComparatorSource.reducedType();
    }

    @Override
    public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) throws IOException {
        return collector.getFieldComparator(numHits, sortPos, reversed);
    }

}
