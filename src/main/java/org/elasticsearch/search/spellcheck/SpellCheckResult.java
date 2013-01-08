package org.elasticsearch.search.spellcheck;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Top level spellcheck result, contains the result for each spellcheck command.
 */
public class SpellCheckResult implements Streamable, ToXContent {

    private List<CommandResult> commands = new ArrayList<CommandResult>(2);

    public SpellCheckResult() {
    }

    public SpellCheckResult(List<CommandResult> commands) {
        this.commands = commands;
    }

    public void addCommand(CommandResult result) {
        commands.add(result);
    }

    public List<CommandResult> commands() {
        return commands;
    }

    public List<CommandResult> getCommands() {
        return commands();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        commands = new ArrayList<CommandResult>(size);
        for (int i = 0; i < size; i++) {
            CommandResult commandResult = new CommandResult();
            commandResult.readFrom(in);
            commands.add(commandResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(commands.size());
        for (CommandResult command : commands) {
            command.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("spellcheck");
        for (CommandResult command : commands) {
            command.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    public static SpellCheckResult readSpellCheck(StreamInput in) throws IOException {
        SpellCheckResult result = new SpellCheckResult();
        result.readFrom(in);
        return result;
    }

    /**
     *
     */
    public static class CommandResult implements Streamable, ToXContent {

        private Text name;
        private int numSuggest;
        private SpellcheckSort sort;
        private Map<Text, List<SuggestedWord>> suggestedWords = newHashMap();

        private Map<String, List<SuggestedWord>> suggestedWordsAsString;

        CommandResult() {
        }

        CommandResult(Text name, int numSuggest, SpellcheckSort sort) {
            this.name = name;
            this.numSuggest = numSuggest;
            this.sort = sort;
        }

        void addSuggestedWord(Text term, SuggestedWord suggestWord) {
            List<SuggestedWord> values = suggestedWords.get(term);
            if (values == null) {
                suggestedWords.put(term, values = new ArrayList<SuggestedWord>(4));
            }
            values.add(suggestWord);
        }

        void addSuggestedWord(Text term, List<SuggestedWord> suggestWord) {
            List<SuggestedWord> values = suggestedWords.get(term);
            if (values == null) {
                suggestedWords.put(term, suggestWord);
            } else {
                values.addAll(suggestWord);
            }
        }

        public Text name() {
            return name;
        }

        /**
         * @return The name of the spellcheck command this result belongs to.
         */
        public String getName() {
            return name.string();
        }

        public Map<Text, List<SuggestedWord>> suggestedWords() {
            return suggestedWords;
        }

        /**
         * @return The actual suggestions per term of the original spellcheck text.
         */
        public Map<String, List<SuggestedWord>> getSuggestedWords() {
            if (suggestedWordsAsString != null) {
                return suggestedWordsAsString;
            }

            suggestedWordsAsString = new HashMap<String, List<SuggestedWord>>();
            for (Map.Entry<Text, List<SuggestedWord>> entry : suggestedWords.entrySet()) {
                suggestedWordsAsString.put(entry.getKey().string(), entry.getValue());
            }
            return suggestedWordsAsString;
        }

        public void aggregate(CommandResult other) {
            assert name().equals(other.name());
            for (Map.Entry<Text, List<SuggestedWord>> entry : other.suggestedWords().entrySet()) {
                List<SuggestedWord> thisSuggestedWords = suggestedWords.get(entry.getKey());
                if (thisSuggestedWords == null) {
                    suggestedWords.put(entry.getKey(), entry.getValue());
                    continue;
                }

                Comparator<SuggestedWord> comparator;
                switch (sort) {
                    case SCORE_FIRST:
                        comparator = new Comparator<SuggestedWord>() {
                            @Override
                            public int compare(SuggestedWord first, SuggestedWord second) {
                                // first criteria: the distance
                                int cmp = Float.compare(first.score(), second.score());
                                if (cmp != 0) {
                                    return cmp;
                                }

                                // second criteria (if first criteria is equal): the popularity
                                cmp = first.frequency() - second.frequency();
                                if (cmp != 0) {
                                    return cmp;
                                }
                                // third criteria: term text
                                return second.suggestion().compareTo(first.suggestion());
                            }
                        };
                        break;
                    case FREQUENCY_FIRST:
                        comparator = new Comparator<SuggestedWord>() {
                            @Override
                            public int compare(SuggestedWord first, SuggestedWord second) {
                                // first criteria: the popularity
                                int cmp = first.frequency() - second.frequency();
                                if (cmp != 0) {
                                    return cmp;
                                }

                                // second criteria (if first criteria is equal): the distance
                                cmp = Float.compare(first.score(), second.score());
                                if (cmp != 0) {
                                    return cmp;
                                }

                                // third criteria: term text
                                return second.suggestion().compareTo(first.suggestion());
                            }
                        };
                        break;
                    default:
                        throw new ElasticSearchException("Could not resolve comparator in reduce phase.");

                }

                NavigableSet<SuggestedWord> mergedSuggestedWords = new TreeSet<SuggestedWord>(comparator);
                mergedSuggestedWords.addAll(thisSuggestedWords);
                mergedSuggestedWords.addAll(entry.getValue());

                int suggestionsToRemove = Math.max(0, mergedSuggestedWords.size() - numSuggest);
                for (int i = 0; i < suggestionsToRemove; i++) {
                    mergedSuggestedWords.pollLast();
                }
                suggestedWords.put(entry.getKey(), new ArrayList<SuggestedWord>(mergedSuggestedWords));
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readText();
            numSuggest = in.readVInt();
            sort = SpellcheckSort.fromId(in.readByte());
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                Text term = in.readText();
                int suggestedWords = in.readVInt();
                List<SuggestedWord> words = new ArrayList<SuggestedWord>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    words.add(SuggestedWord.create(in));
                }
                this.suggestedWords.put(term, words);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeText(name);
            out.writeVInt(numSuggest);
            out.writeByte(sort.id());
            out.writeVInt(suggestedWords.size());
            for (Map.Entry<Text, List<SuggestedWord>> entry : suggestedWords.entrySet()) {
                out.writeText(entry.getKey());
                out.writeVInt(entry.getValue().size());
                for (SuggestedWord word : entry.getValue()) {
                    word.writeTo(out);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(name.string());
            for (Map.Entry<Text, List<SuggestedWord>> entry : suggestedWords.entrySet()) {
                builder.field(entry.getKey().string());
                builder.startArray("suggestion");
                for (SuggestedWord word : entry.getValue()) {
                    builder.startObject();
                    builder.field("suggestion", word.suggestion());
                    builder.field("frequency", word.frequency());
                    builder.field("score", word.score());
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }

}
