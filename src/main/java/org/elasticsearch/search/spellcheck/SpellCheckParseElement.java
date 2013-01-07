/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.spellcheck;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Comparator;

/**
 *
 */
public class SpellCheckParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        SearchContextSpellcheck searchContextSpellcheck = new SearchContextSpellcheck();

        String globalType = "direct";
        Analyzer globalAnalyzer = context.mapperService().searchAnalyzer();
        String globalText = null;
        String globalField = null;
        float globalAccuracy = SpellChecker.DEFAULT_ACCURACY;
        int globalNumSuggest = 5;
        SuggestMode globalSuggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
        Comparator<SuggestWord> globalComparator = SuggestWordQueue.DEFAULT_COMPARATOR;
        StringDistance globalStringDistance = DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        boolean globalLowerCaseTerms = true;
        int globalMaxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
        int globalMaxInspections = 5;
        float globalMaxQueryFrequency = 0.01f;
        int globalMinPrefix = 1;
        int globalMinQueryLength = 4;
        float globalThresholdFrequency = 0f;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("type".equals(fieldName)) {
                    globalType = parser.text();
                } else if ("analyzer".equals(fieldName)) {
                    String analyzerName = parser.text();
                    globalAnalyzer = context.mapperService().analysisService().analyzer(analyzerName);
                    if (globalAnalyzer == null) {
                        throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                    }
                } else if ("text".equals(fieldName)) {
                    globalText = parser.text();
                } else if ("field".equals(fieldName)) {
                    globalField = parser.text();
                } else if ("accuracy".equals(fieldName)) {
                    globalAccuracy = parser.floatValue();
                } else if ("num_suggest".equals(fieldName)) {
                    globalNumSuggest = parser.intValue();
                } else if ("suggest_mode".equals(fieldName)) {
                    globalSuggestMode = resolveSuggestMode(parser.text());
                } else if ("comparator".equals(fieldName)) {
                    globalComparator = resolveComparator(parser.text());
                } else if ("string_distance".equals(fieldName)) {
                    globalStringDistance = resolveDistance(parser.text());
                } else if ("lower_case_terms".equals(fieldName)) {
                    globalLowerCaseTerms = parser.booleanValue();
                } else if ("max_edits".equals(fieldName)) {
                    globalMaxEdits = parser.intValue();
                } else if ("max_inspections".equals(fieldName)) {
                    globalMaxInspections = parser.intValue();
                } else if ("max_query_frequency".equals(fieldName)) {
                    globalMaxQueryFrequency = parser.floatValue();
                } else if ("minPrefix".equals(fieldName)) {
                    globalMinPrefix = parser.intValue();
                } else if ("minQueryLength".equals(fieldName)) {
                    globalMinQueryLength = parser.intValue();
                } else if ("thresholdFrequency".equals(fieldName)) {
                    globalThresholdFrequency = parser.floatValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                SearchContextSpellcheck.Command command = new SearchContextSpellcheck.Command();
                searchContextSpellcheck.addCommand(fieldName, command);

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("type".equals(fieldName)) {
                            command.type(parser.text());
                        } else if ("analyzer".equals(fieldName)) {
                            String analyzerName = parser.text();
                            Analyzer analyzer = context.mapperService().analysisService().analyzer(analyzerName);
                            if (analyzer == null) {
                                throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                            }
                            command.spellCheckAnalyzer(analyzer);
                        } else if ("text".equals(fieldName)) {
                            command.spellCheckText(parser.text());
                        } else if ("field".equals(fieldName)) {
                            command.setSpellCheckField(parser.text());
                        } else if ("accuracy".equals(fieldName)) {
                            command.accuracy(parser.floatValue());
                        } else if ("num_suggest".equals(fieldName)) {
                            command.numSuggest(parser.intValue());
                        } else if ("suggest_mode".equals(fieldName)) {
                            command.suggestMode(resolveSuggestMode(parser.text()));
                        } else if ("comparator".equals(fieldName)) {
                            command.comparator(resolveComparator(parser.text()));
                        } else if ("string_distance".equals(fieldName)) {
                            command.stringDistance(resolveDistance(parser.text()));
                        } else if ("lower_case_terms".equals(fieldName)) {
                            command.lowerCaseTerms(parser.booleanValue());
                        } else if ("max_edits".equals(fieldName)) {
                            command.maxEdits(parser.intValue());
                        } else if ("max_inspections".equals(fieldName)) {
                            command.maxInspections(parser.intValue());
                        } else if ("max_query_frequency".equals(fieldName)) {
                            command.maxQueryFrequency(parser.floatValue());
                        } else if ("minPrefix".equals(fieldName)) {
                            command.minPrefix(parser.intValue());
                        } else if ("minQueryLength".equals(fieldName)) {
                            command.minQueryLength(parser.intValue());
                        } else if ("thresholdFrequency".equals(fieldName)) {
                            command.thresholdFrequency(parser.floatValue());
                        }
                    }
                }
            }
        }

        for (SearchContextSpellcheck.Command command : searchContextSpellcheck.commands().values()) {
            if (command.type() == null) {
                command.type(globalType);
            }

            if (command.spellCheckAnalyzer() == null) {
                command.spellCheckAnalyzer(globalAnalyzer);
            }
            if (command.spellCheckText() == null) {
                command.spellCheckText(globalText);
            }
            if (command.spellCheckField() == null) {
                command.setSpellCheckField(globalField);
            }
            if (command.accuracy() == null) {
                command.accuracy(globalAccuracy);
            }
            if (command.numSuggest() == null) {
                command.numSuggest(globalNumSuggest);
            }
            if (command.suggestMode() == null) {
                command.suggestMode(globalSuggestMode);
            }
            if (command.comparator() == null) {
                command.comparator(globalComparator);
            }
            if (command.stringDistance() == null) {
                command.stringDistance(globalStringDistance);
            }
            if (command.lowerCaseTerms() == null) {
                command.lowerCaseTerms(globalLowerCaseTerms);
            }
            if (command.maxEdits() == null) {
                command.maxEdits(globalMaxEdits);
            }
            if (command.maxInspections() == null) {
                command.maxInspections(globalMaxInspections);
            }
            if (command.maxQueryFrequency() == null) {
                command.maxQueryFrequency(globalMaxQueryFrequency);
            }
            if (command.minPrefix() == null) {
                command.minPrefix(globalMinPrefix);
            }
            if (command.minQueryLength() == null) {
                command.minQueryLength(globalMinQueryLength);
            }
            if (command.thresholdFrequency() == null) {
                command.thresholdFrequency(globalThresholdFrequency);
            }
        }
        context.spellcheck(searchContextSpellcheck);
    }

    private SuggestMode resolveSuggestMode(String suggestModeVal) {
        if ("when_not_in_index".equals(suggestModeVal)) {
            return SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
        } else if ("more_popular".equals(suggestModeVal)) {
            return SuggestMode.SUGGEST_MORE_POPULAR;
        } else if ("always".equals(suggestModeVal)) {
            return SuggestMode.SUGGEST_ALWAYS;
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + suggestModeVal);
        }
    }

    private Comparator<SuggestWord> resolveComparator(String comparatorVal) {
        if ("score_first".equals(comparatorVal)) {
            return SuggestWordQueue.DEFAULT_COMPARATOR;
        } else if ("frequency_first".equals(comparatorVal)) {
            return new SuggestWordFrequencyComparator();
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal comparator option " + comparatorVal);
        }
    }

    private StringDistance resolveDistance(String distanceVal) {
        if ("internal".equals(distanceVal)) {
            return DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        } else if ("damerau_levenshtein".equals(distanceVal)) {
            return new LuceneLevenshteinDistance();
        } else if ("levenstein".equals(distanceVal)) {
            return new LevensteinDistance();
        } else if ("jarowinkler".equals(distanceVal)) {
            return new JaroWinklerDistance();
        } else if ("ngram".equals(distanceVal)) {
            return new NGramDistance();
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal distance option " + distanceVal);
        }
    }

}
