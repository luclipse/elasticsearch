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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
public class SpellCheckPhase extends AbstractComponent implements SearchPhase {

    @Inject
    public SpellCheckPhase(Settings settings) {
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
        SearchContextSpellCheck spellCheck = context.spellcheck();
        if (spellCheck == null) {
            return;
        }
        DirectSpellChecker directSpellChecker = new DirectSpellChecker();
        directSpellChecker.setAccuracy(spellCheck.accuracy());

        try {
            InternalSpellCheckResult result = new InternalSpellCheckResult();
            List<Token> tokens = queryTerms(spellCheck, context);
            for (Token token : tokens) {
                Term term = new Term(spellCheck.spellCheckField(), new String(token.buffer(), 0, token.length()));
                SuggestWord[] suggestWords = directSpellChecker.suggestSimilar(
                        term, spellCheck.numSuggest(), context.searcher().getIndexReader(), spellCheck.suggestMode()
                );
                result.addSuggestedWord(term.text(), suggestWords);
            }
            context.queryResult().spellCheck(result);
        } catch (IOException e) {
            throw new ElasticSearchException("Exception during spellchecking", e);
        }
    }

    private List<Token> queryTerms(SearchContextSpellCheck spellCheck, SearchContext context) throws IOException {
        Analyzer analyzer = spellCheck.spellCheckAnalyzer();
        if (analyzer == null) {
            analyzer = context.mapperService().searchAnalyzer();
        }

        String text = context.spellcheck().spellCheckText();

        TokenStream ts = analyzer.tokenStream("", new StringReader(text));
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
