package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class PercolateShardRequest extends BroadcastShardOperationRequest {

    private String documentIndex;
    private String documentType;
    private BytesReference documentSource;

    public PercolateShardRequest() {
    }

    public PercolateShardRequest(String index, int shardId, PercolateRequest request) {
        super(index, shardId, request);
        this.documentType = request.documentType();
        this.documentSource = request.documentSource();
    }

    public String documentIndex() {
        return documentIndex;
    }

    public void documentIndex(String documentIndex) {
        this.documentIndex = documentIndex;
    }

    public String documentType() {
        return documentType;
    }

    public void documentType(String type) {
        this.documentType = type;
    }

    public BytesReference documentSource() {
        return documentSource;
    }

    public void documentSource(BytesReference documentSource) {
        this.documentSource = documentSource;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        documentIndex = in.readOptionalString();
        documentType = in.readString();
        documentSource = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(documentIndex);
        out.writeString(documentType);
        out.writeBytesReference(documentSource);
    }

}
