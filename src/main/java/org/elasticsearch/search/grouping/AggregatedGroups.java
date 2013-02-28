package org.elasticsearch.search.grouping;

import org.apache.lucene.search.grouping.SearchGroup;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class AggregatedGroups implements Streamable {

    private Collection<SearchGroup> searchGroups;

    public AggregatedGroups() {
    }

    public AggregatedGroups(Collection<SearchGroup> searchGroups) {
        this.searchGroups = searchGroups;
    }

    public Collection<SearchGroup> searchGroups() {
        return searchGroups;
    }

    public void searchGroups(Collection<SearchGroup> searchGroups) {
        this.searchGroups = searchGroups;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        searchGroups = (Collection) Lucene.readSearchGroups(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Lucene.writeSearchGroups((Collection) searchGroups, out);
    }
}
