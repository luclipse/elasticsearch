package org.elasticsearch.search.fetch.nested;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class NestedSearchHits implements Streamable, ToXContent {

    private int totalHits;
    private float maxScore;
    private NestedSearchHit[] hits;

    public NestedSearchHits(int totalHits, float maxScore, NestedSearchHit[] hits) {
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.hits = hits;
    }

    NestedSearchHits() {
    }

    public int totalHits() {
        return totalHits;
    }

    public int getTotalHits() {
        return totalHits();
    }

    public float maxScore() {
        return maxScore;
    }

    public float getMaxScore() {
        return maxScore();
    }

    public NestedSearchHit[] hits() {
        return hits;
    }

    public NestedSearchHit[] getHits() {
        return hits();
    }

    public NestedSearchHit getAt(int position) {
        return hits[position];
    }

    public static NestedSearchHits read(StreamInput in) throws IOException {
        NestedSearchHits newHits = new NestedSearchHits();
        newHits.readFrom(in);
        return newHits;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalHits = in.readVInt();
        maxScore = in.readFloat();
        int size = in.readVInt();
        hits = new NestedSearchHit[size];
        for (int i = 0; i < size; i++) {
            hits[i] = NestedSearchHit.read(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalHits);
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        for (NestedSearchHit hit : hits) {
            hit.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOTAL, totalHits);
        builder.field(Fields.MAX_SCORE, maxScore);
        builder.startArray(Fields.HITS);
        for (NestedSearchHit nestedSearchHit : hits) {
            nestedSearchHit.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static class Fields {

        static final XContentBuilderString HITS = new XContentBuilderString("hits");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MAX_SCORE = new XContentBuilderString("max_score");

    }
}
