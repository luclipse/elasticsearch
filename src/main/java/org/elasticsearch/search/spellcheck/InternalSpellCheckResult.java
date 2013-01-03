package org.elasticsearch.search.spellcheck;

import org.apache.lucene.search.spell.SuggestWord;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 */
public class InternalSpellCheckResult implements Streamable, ToXContent {

    private final Map<String, List<SuggestWord>> suggestedWords;

    public InternalSpellCheckResult() {
        suggestedWords = newHashMap();
    }

    public InternalSpellCheckResult(Map<String, List<SuggestWord>> suggestedWords) {
        this.suggestedWords = suggestedWords;
    }

    public void addSuggestedWord(String term, SuggestWord... suggestWords) {
        suggestedWords.put(term, Arrays.asList(suggestWords));
    }

    public Map<String, List<SuggestWord>> suggestedWords() {
        return suggestedWords;
    }

    public Map<String, List<SuggestWord>> getSuggestedWords() {
        return suggestedWords();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String term = in.readString();
            int suggestedWords = in.readVInt();
            List<SuggestWord> words = new ArrayList<SuggestWord>(suggestedWords);
            for (int j = 0; j < suggestedWords; j++) {
                SuggestWord word = new SuggestWord();
                word.string = in.readString();
                word.freq = in.readVInt();
                word.score = in.readFloat();
                words.add(word);
            }
            this.suggestedWords.put(term, words);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(suggestedWords.size());
        for (Map.Entry<String, List<SuggestWord>> entry : suggestedWords.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (SuggestWord word : entry.getValue()) {
                out.writeString(word.string);
                out.writeVInt(word.freq);
                out.writeFloat(word.score);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("spellcheck");
        for (Map.Entry<String, List<SuggestWord>> entry : suggestedWords.entrySet()) {
            builder.field(entry.getKey());
            builder.startArray("suggestion");
            for (SuggestWord word : entry.getValue()) {
                builder.startObject();
                builder.field("word", word.string);
                builder.field("freq", word.freq);
                builder.field("score", word.score);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static InternalSpellCheckResult readSpellCheck(StreamInput in) throws IOException {
        InternalSpellCheckResult result = new InternalSpellCheckResult();
        result.readFrom(in);
        return result;
    }
}
