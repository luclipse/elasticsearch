package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private String[] matches;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(String[] matches, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
    }

    public String[] matches() {
        return matches;
    }

    public void matches(String[] matches) {
        this.matches = matches;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        matches = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(matches);
    }
}
