package org.elasticsearch.search.spellcheck;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        private Map<Text, List<SuggestedWord>> suggestedWords;

        private Map<String, List<SuggestedWord>> suggestedWordsAsString;

        CommandResult() {
            this.suggestedWords = newHashMap();
        }

        CommandResult(Text name) {
            this.name = name;
            this.suggestedWords = newHashMap();
        }

        void addSuggestedWord(Text term, SuggestedWord suggestWord) {
            List<SuggestedWord> values = suggestedWords.get(term);
            if (values == null) {
                suggestedWords.put(term, values = new ArrayList<SuggestedWord>(4));
            }
            values.add(suggestWord);
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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readText();
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
