package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class SpellCheckBuilder implements ToXContent {

    private String spellCheckText;
    private String spellCheckField;
    private String spellCheckAnalyzer;
    private String suggestMode;
    private Float accuracy;
    private Integer numSuggest;

    public SpellCheckBuilder setSpellCheckText(String spellCheckText) {
        this.spellCheckText = spellCheckText;
        return this;
    }

    public SpellCheckBuilder setSpellCheckField(String spellCheckField) {
        this.spellCheckField = spellCheckField;
        return this;
    }

    public SpellCheckBuilder setSpellCheckAnalyzer(String spellCheckAnalyzer) {
        this.spellCheckAnalyzer = spellCheckAnalyzer;
        return this;
    }

    public SpellCheckBuilder setSuggestMode(String suggestMode) {
        this.suggestMode = suggestMode;
        return this;
    }

    public SpellCheckBuilder setAccuracy(float accuracy) {
        this.accuracy = accuracy;
        return this;
    }

    public SpellCheckBuilder setNumSuggest(int numSuggest) {
        this.numSuggest = numSuggest;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("spellcheck");
        if (spellCheckAnalyzer != null) {
            builder.field("analyzer", spellCheckAnalyzer);
        }
        if (spellCheckText != null) {
            builder.field("text", spellCheckText);
        }
        if (spellCheckField != null) {
            builder.field("field", spellCheckField);
        }
        if (suggestMode != null) {
            builder.field("suggest_mode", suggestMode);
        }
        if (accuracy != null) {
            builder.field("accuracy", accuracy);
        }
        if (numSuggest != null) {
            builder.field("num_suggest", numSuggest);
        }
        builder.endObject();
        return builder;
    }
}
