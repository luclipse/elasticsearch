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
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SpellCheckParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        SearchContextSpellcheck searchContextSpellcheck = new SearchContextSpellcheck();

        Analyzer globalAnalyzer = context.mapperService().searchAnalyzer();
        String globalText = null;
        String globalField = null;
        float globalAccuracy = SpellChecker.DEFAULT_ACCURACY;
        int globalNumSuggest = 5;
        SuggestMode globalSuggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("analyzer".equals(fieldName)) {
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
                    String suggestModeVal = parser.text();
                    if ("when_not_in_index".equals(suggestModeVal)) {
                        globalSuggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
                    } else if ("more_popular".equals(suggestModeVal)) {
                        globalSuggestMode = SuggestMode.SUGGEST_MORE_POPULAR;
                    } else if ("always".equals(suggestModeVal)) {
                        globalSuggestMode = SuggestMode.SUGGEST_ALWAYS;
                    } else {
                        throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + suggestModeVal);
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                SearchContextSpellcheck.Command command = new SearchContextSpellcheck.Command();
                searchContextSpellcheck.addCommand(fieldName, command);

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("analyzer".equals(fieldName)) {
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
                            String suggestModeVal = parser.text();
                            if ("when_not_in_index".equals(suggestModeVal)) {
                                command.suggestMode(SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
                            } else if ("more_popular".equals(suggestModeVal)) {
                                command.suggestMode(SuggestMode.SUGGEST_MORE_POPULAR);
                            } else if ("always".equals(suggestModeVal)) {
                                command.suggestMode(SuggestMode.SUGGEST_ALWAYS);
                            } else {
                                throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + suggestModeVal);
                            }
                        }
                    }
                }
            }
        }

        for (SearchContextSpellcheck.Command command : searchContextSpellcheck.commands().values()) {
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
        }
        context.spellcheck(searchContextSpellcheck);
    }
}
