package org.elasticsearch.search.spellcheck;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
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
            CharsRef spare = new CharsRef();
            SpellCheckResult result = new SpellCheckResult();
            for (Map.Entry<String, SearchContextSpellcheck.Command> entry : spellCheck.commands().entrySet()) {
                SearchContextSpellcheck.Command command = entry.getValue();
                if ("direct".equals(command.type())) {
                    if (command.filter() != null) {
                        result.addCommand(executeWithFilter(entry.getKey(), command, context, spare));
                    } else {
                        result.addCommand(executeWithoutFilter(entry.getKey(), command, context, spare));
                    }
                } else {
                    throw new ElasticSearchIllegalArgumentException("Unsupported spellcheck type[" + command.type() + "]");
                }
            }
            context.queryResult().spellCheck(result);
        } catch (IOException e) {
            throw new ElasticSearchException("I/O exception during spellchecking", e);
        }
    }

    private SpellCheckResult.CommandResult executeWithoutFilter(String commandName, SearchContextSpellcheck.Command command, SearchContext context, CharsRef spare) throws IOException {
        DirectSpellChecker directSpellChecker = new DirectSpellChecker();
        directSpellChecker.setAccuracy(command.accuracy());
        Comparator<SuggestWord> comparator;
        switch (command.sort()) {
            case SCORE_FIRST:
                comparator = SuggestWordQueue.DEFAULT_COMPARATOR;
                break;
            case FREQUENCY_FIRST:
                comparator = new SuggestWordFrequencyComparator();
                break;
            default:
                throw new ElasticSearchIllegalArgumentException("Illegal spellcheck sort: " + command.sort());
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

        SpellCheckResult.CommandResult commandResult = new SpellCheckResult.CommandResult(
                new StringText(commandName), command.numSuggest(), command.sort()
        );
        List<Term> tokens = queryTerms(command, spare);
        for (Term term : tokens) {
            IndexReader indexReader = context.searcher().getIndexReader();
            SuggestWord[] suggestedWords = directSpellChecker.suggestSimilar(
                    term, command.numSuggest(), indexReader, command.suggestMode()
            );
            Text key = new BytesText(new BytesArray(term.bytes()));
            for (SuggestWord suggestWord : suggestedWords) {
                Text word = new StringText(suggestWord.string);
                commandResult.addSuggestedWord(key, new SuggestedWord(word, suggestWord.freq, suggestWord.score));
            }
        }
        return commandResult;
    }

    private SpellCheckResult.CommandResult executeWithFilter(String commandName, SearchContextSpellcheck.Command command, SearchContext context, CharsRef spare) throws IOException {
        ForkedDirectSpellChecker directSpellChecker = new ForkedDirectSpellChecker();
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
                throw new ElasticSearchIllegalArgumentException("Illegal spellcheck sort: " + command.sort());
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
        directSpellChecker.setFilter(command.filter());

        SpellCheckResult.CommandResult commandResult = new SpellCheckResult.CommandResult(
                new StringText(commandName), command.numSuggest(), command.sort()
        );
        List<Term> tokens = queryTerms(command, spare);
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
        return commandResult;
    }

    private List<Term> queryTerms(SearchContextSpellcheck.Command command, CharsRef spare) throws IOException {
        UnicodeUtil.UTF8toUTF16(command.spellCheckText(), spare);
        TokenStream ts = command.spellCheckAnalyzer().tokenStream(
                command.spellCheckField(), new FastCharArrayReader(spare.chars, spare.offset, spare.length)
        );
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
