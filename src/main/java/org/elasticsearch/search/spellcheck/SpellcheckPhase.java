package org.elasticsearch.search.spellcheck;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class SpellcheckPhase extends AbstractComponent implements SearchPhase {

    @Inject
    public SpellcheckPhase(Settings settings) {
        super(settings);
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("spellcheck", new SpellCheckParseElement());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        SearchContextSpellcheck spellCheck = context.spellcheck();
        if (spellCheck == null) {
            return;
        }

        try {
            SpellCheckResult result = new SpellCheckResult();
            for (Map.Entry<String, SearchContextSpellcheck.Command> entry : spellCheck.commands().entrySet()) {
                SearchContextSpellcheck.Command command = entry.getValue();
                if ("direct".equals(command.type())) {
                    DirectSpellChecker directSpellChecker = new DirectSpellChecker();
                    directSpellChecker.setAccuracy(command.accuracy());
                    directSpellChecker.setComparator(command.comparator());
                    directSpellChecker.setDistance(command.stringDistance());
                    directSpellChecker.setLowerCaseTerms(command.lowerCaseTerms());
                    directSpellChecker.setMaxEdits(command.maxEdits());
                    directSpellChecker.setMaxInspections(command.maxInspections());
                    directSpellChecker.setMaxQueryFrequency(command.maxQueryFrequency());
                    directSpellChecker.setMinPrefix(command.minPrefix());
                    directSpellChecker.setMinQueryLength(command.minQueryLength());
                    directSpellChecker.setThresholdFrequency(command.thresholdFrequency());

                    SpellCheckResult.CommandResult commandResult = new SpellCheckResult.CommandResult(new StringText(entry.getKey()));
                    List<Token> tokens = queryTerms(command, context);
                    for (Token token : tokens) {
                        Term term = new Term(command.spellCheckField(), new String(token.buffer(), 0, token.length()));
                        // TODO: tweak DirectSpellChecker to allow us to get words as BR instead of String
                        SuggestWord[] suggestWords = directSpellChecker.suggestSimilar(
                                term, command.numSuggest(), context.searcher().getIndexReader(), command.suggestMode()
                        );
                        Text key = new BytesText(new BytesArray(term.bytes()));
                        for (SuggestWord suggestWord : suggestWords) {
                            SuggestedWord suggestedWord = new SuggestedWord(
                                    new StringText(suggestWord.string), suggestWord.freq, suggestWord.score
                            );
                            commandResult.addSuggestedWord(key, suggestedWord);
                        }
                    }
                    result.addCommand(commandResult);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Type " + command.type() + " is not supported");
                }
            }
            context.queryResult().spellCheck(result);
        } catch (IOException e) {
            throw new ElasticSearchException("Exception during spellchecking", e);
        }
    }

    private List<Token> queryTerms(SearchContextSpellcheck.Command command, SearchContext context) throws IOException {
        Analyzer analyzer = command.spellCheckAnalyzer();
        TokenStream ts = analyzer.tokenStream(command.spellCheckField(), new FastStringReader(command.spellCheckText()));
        ts.reset();
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);

        List<Token> result = new ArrayList<Token>(5);
        while (ts.incrementToken()) {
            Token token = new Token();
            token.copyBuffer(termAtt.buffer(), 0, termAtt.length());
            result.add(token);
        }
        return result;
    }

}
