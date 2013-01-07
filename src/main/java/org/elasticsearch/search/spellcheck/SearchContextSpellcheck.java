package org.elasticsearch.search.spellcheck;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.SuggestMode;

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

        private String spellCheckText;
        private String spellCheckField;
        private Analyzer spellCheckAnalyzer;
        private SuggestMode suggestMode;
        private Float accuracy;
        private Integer numSuggest;

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

    }

}
