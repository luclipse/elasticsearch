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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.StringReader;
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
            DirectSpellChecker directSpellChecker = new DirectSpellChecker();
            SpellCheckResult result = new SpellCheckResult();
            for (Map.Entry<String, SearchContextSpellcheck.Command> entry : spellCheck.commands().entrySet()) {
                SearchContextSpellcheck.Command command = entry.getValue();
                directSpellChecker.setAccuracy(command.accuracy());

                SpellCheckResult.CommandResult commandResult = new SpellCheckResult.CommandResult(new StringText(entry.getKey()));
                List<Token> tokens = queryTerms(command, context);
                for (Token token : tokens) {
                    Term term = new Term(command.spellCheckField(), new String(token.buffer(), 0, token.length()));
                    // TODO: tweak DirectSpellChecker to allow us to get words as BR instead of String
                    // actually remove the SuggestWord usage
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
            }
            context.queryResult().spellCheck(result);
        } catch (IOException e) {
            throw new ElasticSearchException("Exception during spellchecking", e);
        }
    }

    private List<Token> queryTerms(SearchContextSpellcheck.Command command, SearchContext context) throws IOException {
        Analyzer analyzer = command.spellCheckAnalyzer();
        TokenStream ts = analyzer.tokenStream(command.spellCheckField(), new StringReader(command.spellCheckText()));
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
