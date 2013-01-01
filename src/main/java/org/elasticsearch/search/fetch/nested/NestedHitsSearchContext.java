package org.elasticsearch.search.fetch.nested;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.util.List;
import java.util.Map;

/**
 */
public class NestedHitsSearchContext {

    private final Map<String, NestedHit> nestedHits;
    private final boolean sourceRequired;

    public NestedHitsSearchContext(Map<String, NestedHit> nestedHits, boolean sourceRequired) {
        this.nestedHits = nestedHits;
        this.sourceRequired = sourceRequired;
    }

    public Map<String, NestedHit> nestedHits() {
        return nestedHits;
    }

    public boolean sourceRequired() {
        return sourceRequired;
    }

    public static class NestedHit {

        private final String path;
        private final Query childQuery;
        private final Filter childFilter;
        private final Sort innerSort;
        private final int size;
        private final int offset;
        private final List<String> fields;

        public NestedHit(String path, Query childQuery, Filter childFilter, Sort innerSort, int size, int offset, List<String> fields) {
            this.path = path;
            this.childQuery = childQuery;
            this.childFilter = childFilter;
            this.innerSort = innerSort;
            this.size = size;
            this.offset = offset;
            this.fields = fields;
        }

        public String path() {
            return path;
        }

        public Query childQuery() {
            return childQuery;
        }

        public Filter childFilter() {
            return childFilter;
        }

        public Sort innerSort() {
            return innerSort;
        }

        public int size() {
            return size;
        }

        public int offset() {
            return offset;
        }

        public List<String> fields() {
            return fields;
        }

        public boolean sourceRequired() {
            return fields == null || fields.contains("_source");
        }

    }
}
