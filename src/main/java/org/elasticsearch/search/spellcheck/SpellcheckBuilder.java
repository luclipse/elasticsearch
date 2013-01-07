package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SpellcheckBuilder implements ToXContent {

    private String globalType;
    private String globalSpellCheckText;
    private String globalSpellCheckField;
    private String globalSpellCheckAnalyzer;
    private String globalSuggestMode;
    private Float globalAccuracy;
    private Integer globalNumSuggest;
    private String globalComparator;
    private String globalStringDistance;
    private Boolean globalLowerCaseTerms;
    private Integer globalMaxEdits;
    private Integer globalMaxInspections;
    private Float globalMaxQueryFrequency;
    private Integer globalMinPrefix;
    private Integer globalMinQueryLength;
    private Float globalThresholdFrequency;

    private final Map<String, Command> commands = new HashMap<String, Command>();

    public SpellcheckBuilder setGlobalType(String globalType) {
        this.globalType = globalType;
        return this;
    }

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

    public SpellcheckBuilder setGlobalComparator(String globalComparator) {
        this.globalComparator = globalComparator;
        return this;
    }

    public SpellcheckBuilder setGlobalStringDistance(String globalStringDistance) {
        this.globalStringDistance = globalStringDistance;
        return this;
    }

    public SpellcheckBuilder setGlobalLowerCaseTerms(boolean globalLowerCaseTerms) {
        this.globalLowerCaseTerms = globalLowerCaseTerms;
        return this;
    }

    public SpellcheckBuilder setGlobalMaxEdits(int globalMaxEdits) {
        this.globalMaxEdits = globalMaxEdits;
        return this;
    }

    public SpellcheckBuilder setGlobalMaxInspections(int globalMaxInspections) {
        this.globalMaxInspections = globalMaxInspections;
        return this;
    }

    public SpellcheckBuilder setGlobalMaxQueryFrequency(float globalMaxQueryFrequency) {
        this.globalMaxQueryFrequency = globalMaxQueryFrequency;
        return this;
    }

    public SpellcheckBuilder setGlobalMinPrefix(Integer globalMinPrefix) {
        this.globalMinPrefix = globalMinPrefix;
        return this;
    }

    public SpellcheckBuilder setGlobalMinQueryLength(Integer globalMinQueryLength) {
        this.globalMinQueryLength = globalMinQueryLength;
        return this;
    }

    public SpellcheckBuilder setGlobalThresholdFrequency(Float globalThresholdFrequency) {
        this.globalThresholdFrequency = globalThresholdFrequency;
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
        if (globalType != null) {
            builder.field("type", globalType);
        }
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
        if (globalComparator != null) {
            builder.field("comparator", globalComparator);
        }
        if (globalStringDistance != null) {
            builder.field("string_distance", globalStringDistance);
        }
        if (globalLowerCaseTerms != null) {
            builder.field("lower_case_terms", globalLowerCaseTerms);
        }
        if (globalMaxEdits != null) {
            builder.field("max_edits", globalMaxEdits);
        }
        if (globalMaxInspections != null) {
            builder.field("max_inspections", globalMaxInspections);
        }
        if (globalMaxQueryFrequency != null) {
            builder.field("max_query_frequency", globalMaxQueryFrequency);
        }
        if (globalMinPrefix != null) {
            builder.field("min_prefix", globalMinPrefix);
        }
        if (globalMinQueryLength != null) {
            builder.field("min_query_length", globalMinQueryLength);
        }
        if (globalThresholdFrequency != null) {
            builder.field("threshold_frequency", globalThresholdFrequency);
        }

        for (Map.Entry<String, Command> entry : commands.entrySet()) {
            builder.startObject(entry.getKey());
            Command command = entry.getValue();
            if (command.type != null) {
                builder.field("type", command.type);
            }
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
            if (command.comparator != null) {
                builder.field("comparator", command.comparator);
            }
            if (command.stringDistance != null) {
                builder.field("string_distance", command.stringDistance);
            }
            if (command.lowerCaseTerms != null) {
                builder.field("lower_case_terms", command.lowerCaseTerms);
            }
            if (command.maxEdits != null) {
                builder.field("max_edits", command.maxEdits);
            }
            if (command.maxInspections != null) {
                builder.field("max_inspections", command.maxInspections);
            }
            if (command.maxQueryFrequency != null) {
                builder.field("max_query_frequency", command.maxQueryFrequency);
            }
            if (command.minPrefix != null) {
                builder.field("min_prefix", command.minPrefix);
            }
            if (command.minQueryLength != null) {
                builder.field("min_query_length", command.minQueryLength);
            }
            if (command.thresholdFrequency != null) {
                builder.field("threshold_frequency", command.thresholdFrequency);
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    public static Command createCommand() {
        return new Command();
    }

    public static class Command {

        private String type;
        private String spellCheckText;
        private String spellCheckField;
        private String spellCheckAnalyzer;
        private String suggestMode;
        private Float accuracy;
        private Integer numSuggest;
        private String comparator;
        private String stringDistance;
        private Boolean lowerCaseTerms;
        private Integer maxEdits;
        private Integer maxInspections;
        private Float maxQueryFrequency;
        private Integer minPrefix;
        private Integer minQueryLength;
        private Float thresholdFrequency;

        public Command setType(String type) {
            this.type = type;
            return this;
        }

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

        public Command setComparator(String comparator) {
            this.comparator = comparator;
            return this;
        }

        public Command setStringDistance(String stringDistance) {
            this.stringDistance = stringDistance;
            return this;
        }

        public Command setLowerCaseTerms(Boolean lowerCaseTerms) {
            this.lowerCaseTerms = lowerCaseTerms;
            return this;
        }

        public Command setMaxEdits(Integer maxEdits) {
            this.maxEdits = maxEdits;
            return this;
        }

        public Command setMaxInspections(Integer maxInspections) {
            this.maxInspections = maxInspections;
            return this;
        }

        public Command setMaxQueryFrequency(Float maxQueryFrequency) {
            this.maxQueryFrequency = maxQueryFrequency;
            return this;
        }

        public Command setMinPrefix(Integer minPrefix) {
            this.minPrefix = minPrefix;
            return this;
        }

        public Command setMinQueryLength(Integer minQueryLength) {
            this.minQueryLength = minQueryLength;
            return this;
        }

        public Command setThresholdFrequency(Float thresholdFrequency) {
            this.thresholdFrequency = thresholdFrequency;
            return this;
        }
    }

}
