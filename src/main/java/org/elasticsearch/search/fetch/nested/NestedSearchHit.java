package org.elasticsearch.search.fetch.nested;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHitField;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.internal.InternalSearchHitField.readSearchHitField;

/**
 */
public class NestedSearchHit implements Streamable, ToXContent {

    private int offset;
    private float score;
    private BytesReference nestedSource;
    private Map<String, SearchHitField> nestedHits;

    public NestedSearchHit(int offset, float score, BytesReference nestedSource, Map<String, SearchHitField> nestedHits) {
        this.offset = offset;
        this.score = score;
        this.nestedSource = nestedSource;
        this.nestedHits = nestedHits;
    }

    NestedSearchHit() {
    }

    public int offset() {
        return offset;
    }

    public int getOffset() {
        return offset;
    }

    public float score() {
        return score;
    }

    public float getScore() {
        return score;
    }

    public BytesReference nestedSource() {
        return nestedSource;
    }

    public BytesReference getNestedSource() {
        return nestedSource;
    }

    public Map<String, SearchHitField> nestedHits() {
        return nestedHits;
    }

    public Map<String, SearchHitField> getNestedHits() {
        return nestedHits;
    }

    static NestedSearchHit read(StreamInput in) throws IOException {
        NestedSearchHit hit = new NestedSearchHit();
        hit.readFrom(in);
        return hit;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        offset = in.readVInt();
        score = in.readFloat();
        nestedSource = in.readBytesReference();
//        if (nestedSource.length() == 0) {
//            nestedSource = null;
//        }

        int size = in.readVInt();
        if (size > 0) {
            ImmutableMap.Builder<String, SearchHitField> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                SearchHitField hitField = readSearchHitField(in);
                builder.put(hitField.name(), hitField);
            }
            nestedHits = builder.build();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(offset);
        out.writeFloat(score);
        out.writeBytesReference(nestedSource);
        if (nestedHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(nestedHits.size());
            for (SearchHitField hitField : nestedHits.values()) {
                hitField.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.OFFSET, offset);
        builder.field(Fields.SCORE, score);
        if (nestedSource != null) {
            XContentHelper.writeRawField("_source", nestedSource, builder, params);
        }
        if (nestedHits != null && !nestedHits.isEmpty()) {
            builder.startObject(Fields.FIELDS);
            for (SearchHitField field : nestedHits.values()) {
                if (field.values().isEmpty()) {
                    continue;
                }
                if (field.values().size() == 1) {
                    builder.field(field.name(), field.values().get(0));
                } else {
                    builder.field(field.name());
                    builder.startArray();
                    for (Object value : field.values()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static class Fields {

        static final XContentBuilderString OFFSET = new XContentBuilderString("_offset");
        static final XContentBuilderString SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");

    }

}
