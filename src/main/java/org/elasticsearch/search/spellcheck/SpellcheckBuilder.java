package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SpellcheckBuilder implements ToXContent {

    private String globalSpellCheckText;
    private String globalSpellCheckField;
    private String globalSpellCheckAnalyzer;
    private String globalSuggestMode;
    private Float globalAccuracy;
    private Integer globalNumSuggest;

    private final Map<String, Command> commands = new HashMap<String, Command>();

    public SpellcheckBuilder setGlobalSpellCheckText(String globalSpellCheckText) {
        this.globalSpellCheckText = globalSpellCheckText;
        return this;
    }

    public SpellcheckBuilder setGlobalSpellCheckField(String globalSpellCheckField) {
        this.globalSpellCheckField = globalSpellCheckField;
        return this;
    }

    public SpellcheckBuilder setGlobalSpellCheckAnalyzer(String globalSpellCheckAnalyzer) {
        this.globalSpellCheckAnalyzer = globalSpellCheckAnalyzer;
        return this;
    }

    public SpellcheckBuilder setGlobalSuggestMode(String globalSuggestMode) {
        this.globalSuggestMode = globalSuggestMode;
        return this;
    }

    public SpellcheckBuilder setGlobalAccuracy(float globalAccuracy) {
        this.globalAccuracy = globalAccuracy;
        return this;
    }

    public SpellcheckBuilder setGlobalNumSuggest(int globalNumSuggest) {
        this.globalNumSuggest = globalNumSuggest;
        return this;
    }

    public SpellcheckBuilder addCommand(String name, Command command) {
        commands.put(name, command);
        return this;
    }

    public Map<String, Command> getCommands() {
        return commands;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("spellcheck");
        if (globalSpellCheckAnalyzer != null) {
            builder.field("analyzer", globalSpellCheckAnalyzer);
        }
        if (globalSpellCheckText != null) {
            builder.field("text", globalSpellCheckText);
        }
        if (globalSpellCheckField != null) {
            builder.field("field", globalSpellCheckField);
        }
        if (globalSuggestMode != null) {
            builder.field("suggest_mode", globalSuggestMode);
        }
        if (globalAccuracy != null) {
            builder.field("accuracy", globalAccuracy);
        }
        if (globalNumSuggest != null) {
            builder.field("num_suggest", globalNumSuggest);
        }

        for (Map.Entry<String, Command> entry : commands.entrySet()) {
            builder.startObject(entry.getKey());
            Command command = entry.getValue();
            if (command.spellCheckAnalyzer != null) {
                builder.field("analyzer", command.spellCheckAnalyzer);
            }
            if (command.spellCheckText != null) {
                builder.field("text", command.spellCheckText);
            }
            if (command.spellCheckField != null) {
                builder.field("field", command.spellCheckField);
            }
            if (command.suggestMode != null) {
                builder.field("suggest_mode", command.suggestMode);
            }
            if (command.accuracy != null) {
                builder.field("accuracy", command.accuracy);
            }
            if (command.numSuggest != null) {
                builder.field("num_suggest", command.numSuggest);
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    public static class Command {

        private String spellCheckText;
        private String spellCheckField;
        private String spellCheckAnalyzer;
        private String suggestMode;
        private Float accuracy;
        private Integer numSuggest;

        public Command setSpellCheckText(String spellCheckText) {
            this.spellCheckText = spellCheckText;
            return this;
        }

        public Command setSpellCheckField(String spellCheckField) {
            this.spellCheckField = spellCheckField;
            return this;
        }

        public Command setSpellCheckAnalyzer(String spellCheckAnalyzer) {
            this.spellCheckAnalyzer = spellCheckAnalyzer;
            return this;
        }

        public Command setSuggestMode(String suggestMode) {
            this.suggestMode = suggestMode;
            return this;
        }

        public Command setAccuracy(float accuracy) {
            this.accuracy = accuracy;
            return this;
        }

        public Command setNumSuggest(int numSuggest) {
            this.numSuggest = numSuggest;
            return this;
        }

    }

}
