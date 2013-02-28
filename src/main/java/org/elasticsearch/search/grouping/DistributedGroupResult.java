package org.elasticsearch.search.grouping;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class DistributedGroupResult extends TransportResponse implements SearchPhaseResult {

    private long id;
    private SearchShardTarget shardTarget;
    private Collection<SearchGroup> searchGroups;

    private int groupOffset;
    private int groupSize;
    private Sort groupSort;

    public DistributedGroupResult() {
    }

    public DistributedGroupResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public Collection<SearchGroup> searchGroups() {
        return searchGroups;
    }

    public void searchGroups(Collection<SearchGroup> searchGroups) {
        this.searchGroups = searchGroups;
    }

    public int groupOffset() {
        return groupOffset;
    }

    public void groupOffset(int groupOffset) {
        this.groupOffset = groupOffset;
    }

    public int groupSize() {
        return groupSize;
    }

    public void groupSize(int groupSize) {
        this.groupSize = groupSize;
    }

    public Sort groupSort() {
        return groupSort;
    }

    public void groupSort(Sort groupSort) {
        this.groupSort = groupSort;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readVLong();
        searchGroups = (Collection) Lucene.readSearchGroups(in);
        groupOffset = in.readVInt();
        groupSize = in.readVInt();
        groupSort = Lucene.readSort(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(id);
        Lucene.writeSearchGroups((Collection) searchGroups, out);
        out.writeVInt(groupOffset);
        out.writeVInt(groupSize);
        Lucene.writeSort(groupSort, out);
    }
}
