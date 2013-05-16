package org.elasticsearch.index.search.child.sorting;

import org.elasticsearch.index.search.child.ParentIdCollector;
import org.elasticsearch.search.internal.SearchContext;

/**
 */
public abstract class AbstractChildFieldValuesCollector extends ParentIdCollector {

    protected AbstractChildFieldValuesCollector(String parentType, SearchContext context) {
        super(parentType, context);
    }

    public abstract AbstractChildFieldComparator getFieldComparator(int numHits, int sortPos, boolean reversed);

    public void clear() {
    }

}
