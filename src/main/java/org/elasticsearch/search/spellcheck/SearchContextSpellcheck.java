package org.elasticsearch.search.spellcheck;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SearchContextSpellcheck {

    private final Map<String, Command> commands = new HashMap<String, Command>();

    public void addCommand(String name, Command command) {
        commands.put(name, command);
    }

    public Map<String, Command> commands() {
        return commands;
    }

    public static class Command {

        private String type;
        private String spellCheckText;
        private String spellCheckField;
        private Analyzer spellCheckAnalyzer;
        private SuggestMode suggestMode;
        private Float accuracy;
        private Integer numSuggest;
        private Comparator<SuggestWord> comparator;
        private StringDistance stringDistance;
        private Boolean lowerCaseTerms;
        private Integer maxEdits;
        private Integer maxInspections;
        private Float maxQueryFrequency;
        private Integer minPrefix;
        private Integer minQueryLength;
        private Float thresholdFrequency;

        public String type() {
            return type;
        }

        public void type(String type) {
            this.type = type;
        }

        public String spellCheckText() {
            return spellCheckText;
        }

        public void spellCheckText(String spellCheckText) {
            this.spellCheckText = spellCheckText;
        }

        public Analyzer spellCheckAnalyzer() {
            return spellCheckAnalyzer;
        }

        public void spellCheckAnalyzer(Analyzer spellCheckAnalyzer) {
            this.spellCheckAnalyzer = spellCheckAnalyzer;
        }

        public String spellCheckField() {
            return spellCheckField;
        }

        public void setSpellCheckField(String spellCheckField) {
            this.spellCheckField = spellCheckField;
        }

        public SuggestMode suggestMode() {
            return suggestMode;
        }

        public void suggestMode(SuggestMode suggestMode) {
            this.suggestMode = suggestMode;
        }

        public Float accuracy() {
            return accuracy;
        }

        public void accuracy(float accuracy) {
            this.accuracy = accuracy;
        }

        public Integer numSuggest() {
            return numSuggest;
        }

        public void numSuggest(int numSuggest) {
            this.numSuggest = numSuggest;
        }

        public Comparator<SuggestWord> comparator() {
            return comparator;
        }

        public void comparator(Comparator<SuggestWord> comparator) {
            this.comparator = comparator;
        }

        public StringDistance stringDistance() {
            return stringDistance;
        }

        public void stringDistance(StringDistance distance) {
            this.stringDistance = distance;
        }

        public Boolean lowerCaseTerms() {
            return lowerCaseTerms;
        }

        public void lowerCaseTerms(boolean lowerCaseTerms) {
            this.lowerCaseTerms = lowerCaseTerms;
        }

        public Integer maxEdits() {
            return maxEdits;
        }

        public void maxEdits(int maxEdits) {
            this.maxEdits = maxEdits;
        }

        public Integer maxInspections() {
            return maxInspections;
        }

        public void maxInspections(int maxInspections) {
            this.maxInspections = maxInspections;
        }

        public Float maxQueryFrequency() {
            return maxQueryFrequency;
        }

        public void maxQueryFrequency(float maxQueryFrequency) {
            this.maxQueryFrequency = maxQueryFrequency;
        }

        public Integer minPrefix() {
            return minPrefix;
        }

        public void minPrefix(int minPrefix) {
            this.minPrefix = minPrefix;
        }

        public Integer minQueryLength() {
            return minQueryLength;
        }

        public void minQueryLength(int minQueryLength) {
            this.minQueryLength = minQueryLength;
        }

        public Float thresholdFrequency() {
            return thresholdFrequency;
        }

        public void thresholdFrequency(float thresholdFrequency) {
            this.thresholdFrequency = thresholdFrequency;
        }
    }

}
