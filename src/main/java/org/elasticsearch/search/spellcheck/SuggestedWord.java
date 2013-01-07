package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class SuggestedWord implements Streamable, ToXContent {

    private Text suggestion;
    private int frequency;
    private float score;

    SuggestedWord(Text suggestion, int frequency, float score) {
        this.suggestion = suggestion;
        this.frequency = frequency;
        this.score = score;
    }

    SuggestedWord() {
    }

    public Text suggestion() {
        return suggestion;
    }

    public int frequency() {
        return frequency;
    }

    public float score() {
        return score;
    }

    public String getSuggestion() {
        return suggestion.string();
    }

    public int getFrequency() {
        return frequency();
    }

    public float getScore() {
        return score();
    }

    public static SuggestedWord create(StreamInput in) throws IOException {
        SuggestedWord suggestion = new SuggestedWord();
        suggestion.readFrom(in);
        return suggestion;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        suggestion = in.readText();
        frequency = in.readVInt();
        score = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeText(suggestion);
        out.writeVInt(frequency);
        out.writeFloat(score);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("suggestion", suggestion);
        builder.field("frequency", frequency);
        builder.field("score", score);
        builder.endObject();
        return builder;
    }
}
