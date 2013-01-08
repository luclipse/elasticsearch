package org.elasticsearch.search.spellcheck;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
                    ForkedDirectSpellChecker directSpellChecker = new ForkedDirectSpellChecker();
//                    DirectSpellChecker directSpellChecker = new DirectSpellChecker();
                    directSpellChecker.setAccuracy(command.accuracy());
                    Comparator<SuggestedWord> comparator;
                    switch (command.sort()) {
                        case SCORE_FIRST:
                            comparator = ForkedDirectSpellChecker.scoreFirst;
                            break;
                        case FREQUENCY_FIRST:
                            comparator = ForkedDirectSpellChecker.frequenctFirst;
                            break;
                        default:
                            throw new ElasticSearchIllegalArgumentException("Illegal spellcheck sort" + command.sort());
                    }
                    directSpellChecker.setComparator(comparator);
                    directSpellChecker.setDistance(command.stringDistance());
                    directSpellChecker.setLowerCaseTerms(command.lowerCaseTerms());
                    directSpellChecker.setMaxEdits(command.maxEdits());
                    directSpellChecker.setMaxInspections(command.maxInspections());
                    directSpellChecker.setMaxQueryFrequency(command.maxQueryFrequency());
                    directSpellChecker.setMinPrefix(command.minPrefix());
                    directSpellChecker.setMinQueryLength(command.minQueryLength());
                    directSpellChecker.setThresholdFrequency(command.thresholdFrequency());
//                    directSpellChecker.setFilter(context.filterCache().cache(context.mapperService().searchFilter(context.types())));

                    SpellCheckResult.CommandResult commandResult = new SpellCheckResult.CommandResult(
                            new StringText(entry.getKey()), command.numSuggest(), command.sort()
                    );
                    List<Term> tokens = queryTerms(command, context);
                    for (Term term : tokens) {
                        List<AtomicReaderContext> readerContexts = context.searcher().getIndexReader().leaves();
                        if (!readerContexts.isEmpty()) {
                            List<SuggestedWord> suggestedWords = directSpellChecker.suggestSimilar(
                                    term, command.numSuggest(), readerContexts, command.suggestMode()
                            );
                            Text key = new BytesText(new BytesArray(term.bytes()));
                            commandResult.addSuggestedWord(key, suggestedWords);
                        }
                    }
                    result.addCommand(commandResult);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Type " + command.type() + " is not supported");
                }
            }
            context.queryResult().spellCheck(result);
        } catch (Exception e) {
            throw new ElasticSearchException("Exception during spellchecking", e);
        }
    }

    private List<Term> queryTerms(SearchContextSpellcheck.Command command, SearchContext context) throws IOException {
        Analyzer analyzer = command.spellCheckAnalyzer();
        CharsRef charsRef = new CharsRef();
        UnicodeUtil.UTF8toUTF16(command.spellCheckText(), charsRef);
        TokenStream ts = analyzer.tokenStream(command.spellCheckField(), new FastCharArrayReader(charsRef.chars, charsRef.offset, charsRef.length));
        ts.reset();
        TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        BytesRef term = termAtt.getBytesRef();

        List<Term> result = new ArrayList<Term>(5);
        while (ts.incrementToken()) {
            termAtt.fillBytesRef();
            result.add(new Term(command.spellCheckField(), BytesRef.deepCopyOf(term)));
        }
        return result;
    }

}
