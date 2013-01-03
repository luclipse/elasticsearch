package org.elasticsearch.search.spellcheck;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.SuggestMode;

/**
 */
public class SearchContextSpellCheck {

    private String spellCheckText;
    private String spellCheckField;
    private Analyzer spellCheckAnalyzer;
    private SuggestMode suggestMode;
    private float accuracy;
    private int numSuggest;

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

    public float accuracy() {
        return accuracy;
    }

    public void accuracy(float accuracy) {
        this.accuracy = accuracy;
    }

    public int numSuggest() {
        return numSuggest;
    }

    public void numSuggest(int numSuggest) {
        this.numSuggest = numSuggest;
    }
}
