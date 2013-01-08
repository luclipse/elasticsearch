package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines how to perform spellchecking. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link Command} instances.
 */
public class SpellcheckBuilder implements ToXContent {

    private String globalType;
    private String globalSpellCheckText;
    private String globalSpellCheckField;
    private String globalSpellCheckAnalyzer;
    private String globalSuggestMode;
    private Float globalAccuracy;
    private Integer globalNumSuggest;
    private String globalSort;
    private String globalStringDistance;
    private Boolean globalLowerCaseTerms;
    private Integer globalMaxEdits;
    private Integer globalMaxInspections;
    private Float globalMaxQueryFrequency;
    private Integer globalMinPrefix;
    private Integer globalMinQueryLength;
    private Float globalThresholdFrequency;
    private FilterBuilder globalFilter;

    private final Map<String, Command> commands = new HashMap<String, Command>();

    /**
     * Sets the spellchecker implementation type.
     * The only possible value is 'direct' which is also the defaults.
     */
    public SpellcheckBuilder setGlobalType(String globalType) {
        this.globalType = globalType;
        return this;
    }

    /**
     * Sets the text to spellcheck. The spellcheck text is a required option that needs
     * to be set either via this setter or via the {@link Command}.
     */
    public SpellcheckBuilder setGlobalSpellCheckText(String globalSpellCheckText) {
        this.globalSpellCheckText = globalSpellCheckText;
        return this;
    }

    /**
     * Sets what field to be used to base the spellcheck suggestions on for the spellcheck text.
     */
    public SpellcheckBuilder setGlobalSpellCheckField(String globalSpellCheckField) {
        this.globalSpellCheckField = globalSpellCheckField;
        return this;
    }

    /**
     * Sets the analyzer to analyse to spellcheck text with. Defaults to the search analyzer
     * of the spellcheck field.
     */
    public SpellcheckBuilder setGlobalSpellCheckAnalyzer(String globalSpellCheckAnalyzer) {
        this.globalSpellCheckAnalyzer = globalSpellCheckAnalyzer;
        return this;
    }

    /**
     * Sets the global suggest mode for how to suggest related terms. Three possible values can be specified:
     * <ol>
     * <li><code>when_not_in_index</code> - Only suggest related terms if spellcheck text terms aren't in the index.
     * This is the default.
     * <li><code>more_popular</code> - Only suggest related terms that occur in more docs then the original
     * spellcheck text terms.
     * <li><code>always</code> - Suggest any matching related terms based on the spellcheck text.
     * </ol>
     */
    public SpellcheckBuilder setGlobalSuggestMode(String globalSuggestMode) {
        this.globalSuggestMode = globalSuggestMode;
        return this;
    }

    /**
     * Sets how similar the suggested terms at least need to be compared to the original spellcheck text terms.
     * A value between 0 and 1 can be specified. This value will be compared to the string distance outcome of each
     * suggested related term.
     * <p/>
     * Default is 0.5f.
     */
    public SpellcheckBuilder setGlobalAccuracy(float globalAccuracy) {
        this.globalAccuracy = globalAccuracy;
        return this;
    }

    /**
     * Sets the maximum related terms to be returned per spellcheck text term.
     */
    public SpellcheckBuilder setGlobalNumSuggest(int globalNumSuggest) {
        this.globalNumSuggest = globalNumSuggest;
        return this;
    }

    /**
     * Sets how to sort the suggested terms per spellcheck text term.
     */
    public SpellcheckBuilder setGlobalSort(String globalSort) {
        this.globalSort = globalSort;
        return this;
    }

    /**
     * Sets what string distance implementation to use for comparing how similar suggested terms are.
     * Four possible values can be specified:
     * <ol>
     * <li><code>internal</code> - This is the default and is based on <code>damerau_levenshtein</code>, but
     * highly optimized for comparing string distance for terms inside the index.
     * <li><code>damerau_levenshtein</code> - String distance algorithm based on Damerau-Levenshtein algorithm.
     * <li><code>levenstein</code> - String distance algorithm based on Levenstein edit distance algorithm.
     * <li><code>jarowinkler</code> - String distance algorithm based on Jaro-Winkler algorithm.
     * <li><code>ngram</code> - String distance algorithm based on n-grams.
     * </ol>
     */
    public SpellcheckBuilder setGlobalStringDistance(String globalStringDistance) {
        this.globalStringDistance = globalStringDistance;
        return this;
    }

    /**
     * Sets whether to lowercase the term before suggesting similar terms.
     */
    public SpellcheckBuilder setGlobalLowerCaseTerms(boolean globalLowerCaseTerms) {
        this.globalLowerCaseTerms = globalLowerCaseTerms;
        return this;
    }

    /**
     * Sets the maximum edit distance related terms can have in order to be considered as a suggestion.
     * Can only be a value between 1 and 2. Any other value result in an bad request error being thrown. Defaults to 2.
     */
    public SpellcheckBuilder setGlobalMaxEdits(int globalMaxEdits) {
        this.globalMaxEdits = globalMaxEdits;
        return this;
    }

    /**
     * A factor that is used to multiply with the numSuggestions with in order to inspect more related terms.
     * Can improve accuracy at the cost of performance. Defaults to 5.
     */
    public SpellcheckBuilder setGlobalMaxInspections(int globalMaxInspections) {
        this.globalMaxInspections = globalMaxInspections;
        return this;
    }

    /**
     * Sets a maximum threshold in number of documents a query term can exist in order to be corrected.
     * Can be a relative percentage number (e.g 0.4) or an absolute number to represent document frequencies.
     * If an value higher than 1 is specified then fractional can not be specified. Defaults to 0.01f.
     * <p/>
     * This can be used to exclude high frequency terms from being spellchecked. High frequency terms are usually
     * spelled correctly on top of this this also improves the spellcheck performance.
     */
    public SpellcheckBuilder setGlobalMaxQueryFrequency(float globalMaxQueryFrequency) {
        this.globalMaxQueryFrequency = globalMaxQueryFrequency;
        return this;
    }

    /**
     * Sets the number of minimal prefix characters that must match in order be a candidate suggested term.
     * Defaults to 1. Increasing this number improves spellcheck performance. Usually misspellings don't occur in the
     * beginning of terms.
     */
    public SpellcheckBuilder setGlobalMinPrefix(int globalMinPrefix) {
        this.globalMinPrefix = globalMinPrefix;
        return this;
    }

    /**
     * The minimum length a query term must have in order to be corrected. Defaults to 4.
     */
    public SpellcheckBuilder setGlobalMinQueryLength(int globalMinQueryLength) {
        this.globalMinQueryLength = globalMinQueryLength;
        return this;
    }

    /**
     * Sets a minimal threshold in number of documents a correct term should appear in. This can be specified as
     * an absolute number or as a relative percentage of number of documents. This can improve quality by only suggesting
     * high frequency terms. Defaults to 0f and is not enabled. If a value higher than 1 is specified then the number
     * cannot be fractional.
     */
    public SpellcheckBuilder setGlobalThresholdFrequency(float globalThresholdFrequency) {
        this.globalThresholdFrequency = globalThresholdFrequency;
        return this;
    }

    /**
     * Sets a filter the corrected terms must match in order to be considered a candidate suggested term.
     */
    public SpellcheckBuilder setGlobalFilter(FilterBuilder globalFilter) {
        this.globalFilter = globalFilter;
        return this;
    }

    /**
     * Adds an {@link Command} instance under a user defined name.
     */
    public SpellcheckBuilder addCommand(String name, Command command) {
        commands.put(name, command);
        return this;
    }

    /**
     * Returns all spellcheck commands under the defined names.
     */
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
        if (globalSort != null) {
            builder.field("sort", globalSort);
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
        if (globalFilter != null) {
            builder.field("filter");
            globalFilter.toXContent(builder, params);
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
            if (command.sort != null) {
                builder.field("sort", command.sort);
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
            if (command.filter != null) {
                builder.field("filter");
                command.filter.toXContent(builder, params);
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    /**
     * Convenience factory method.
     */
    public static Command createCommand() {
        return new Command();
    }

    /**
     * Defines the actual spellcheck command. Each command uses the global options unless defined in the spellcheck
     * command itself. All options are the same as the global options, but are only applicable for this spellcheck
     * command.
     */
    public static class Command {

        private String type;
        private String spellCheckText;
        private String spellCheckField;
        private String spellCheckAnalyzer;
        private String suggestMode;
        private Float accuracy;
        private Integer numSuggest;
        private String sort;
        private String stringDistance;
        private Boolean lowerCaseTerms;
        private Integer maxEdits;
        private Integer maxInspections;
        private Float maxQueryFrequency;
        private Integer minPrefix;
        private Integer minQueryLength;
        private Float thresholdFrequency;
        private FilterBuilder filter;

        /**
         * Same as in {@link #setGlobalType(String)}, but in the spellcheck command scope.
         */
        public Command setType(String type) {
            this.type = type;
            return this;
        }

        /**
         * Same as in {@link #setGlobalSpellCheckText(String)}, but in the spellcheck command scope.
         */
        public Command setSpellCheckText(String spellCheckText) {
            this.spellCheckText = spellCheckText;
            return this;
        }

        /**
         * Same as in {@link #setGlobalSpellCheckField(String)}, but in the spellcheck command scope.
         */
        public Command setSpellCheckField(String spellCheckField) {
            this.spellCheckField = spellCheckField;
            return this;
        }

        /**
         * Same as in {@link #setGlobalSpellCheckAnalyzer(String)}, but in the spellcheck command scope.
         */
        public Command setSpellCheckAnalyzer(String spellCheckAnalyzer) {
            this.spellCheckAnalyzer = spellCheckAnalyzer;
            return this;
        }

        /**
         * Same as in {@link #setGlobalSuggestMode(String)}, but in the spellcheck command scope.
         */
        public Command setSuggestMode(String suggestMode) {
            this.suggestMode = suggestMode;
            return this;
        }

        /**
         * Same as in {@link #setGlobalAccuracy(float)}, but in the spellcheck command scope.
         */
        public Command setAccuracy(float accuracy) {
            this.accuracy = accuracy;
            return this;
        }

        /**
         * Same as in {@link #setGlobalNumSuggest(int)}, but in the spellcheck command scope.
         */
        public Command setNumSuggest(int numSuggest) {
            this.numSuggest = numSuggest;
            return this;
        }

        /**
         * Same as in {@link #setGlobalSort(String)}, but in the spellcheck command scope.
         */
        public Command setSort(String sort) {
            this.sort = sort;
            return this;
        }

        /**
         * Same as in {@link #setGlobalStringDistance(String)}, but in the spellcheck command scope.
         */
        public Command setStringDistance(String stringDistance) {
            this.stringDistance = stringDistance;
            return this;
        }

        /**
         * Same as in {@link #setGlobalLowerCaseTerms(boolean)}, but in the spellcheck command scope.
         */
        public Command setLowerCaseTerms(Boolean lowerCaseTerms) {
            this.lowerCaseTerms = lowerCaseTerms;
            return this;
        }

        /**
         * Same as in {@link #setGlobalMaxEdits(int)}, but in the spellcheck command scope.
         */
        public Command setMaxEdits(Integer maxEdits) {
            this.maxEdits = maxEdits;
            return this;
        }

        /**
         * Same as in {@link #setGlobalMaxInspections(int)}, but in the spellcheck command scope.
         */
        public Command setMaxInspections(Integer maxInspections) {
            this.maxInspections = maxInspections;
            return this;
        }

        /**
         * Same as in {@link #setGlobalMaxQueryFrequency(float)}, but in the spellcheck command scope.
         */
        public Command setMaxQueryFrequency(float maxQueryFrequency) {
            this.maxQueryFrequency = maxQueryFrequency;
            return this;
        }

        /**
         * Same as in {@link #setGlobalMinPrefix(int)}, but in the spellcheck command scope.
         */
        public Command setMinPrefix(int minPrefix) {
            this.minPrefix = minPrefix;
            return this;
        }

        /**
         * Same as in {@link #setGlobalMinQueryLength(int)}, but in the spellcheck command scope.
         */
        public Command setMinQueryLength(int minQueryLength) {
            this.minQueryLength = minQueryLength;
            return this;
        }

        /**
         * Same as in {@link #setGlobalThresholdFrequency(float)}, but in the spellcheck command scope.
         */
        public Command setThresholdFrequency(float thresholdFrequency) {
            this.thresholdFrequency = thresholdFrequency;
            return this;
        }

        public Command setFilter(FilterBuilder filter) {
            this.filter = filter;
            return this;
        }
    }

}
