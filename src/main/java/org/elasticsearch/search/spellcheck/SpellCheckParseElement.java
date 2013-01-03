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
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SpellCheckParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        Analyzer analyzer = context.mapperService().analysisService().defaultAnalyzer();
        String text = null;
        String field = null;
        float accuracy = SpellChecker.DEFAULT_ACCURACY;
        int numSuggest = 5;
        SuggestMode suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                if ("analyzer".equals(fieldName)) {
                    parser.nextToken();
                    String analyzerName = parser.text();
                    analyzer = context.mapperService().analysisService().analyzer(analyzerName);
                    if (analyzer == null) {
                        throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                    }
                } else if ("text".equals(fieldName)) {
                    parser.nextToken();
                    text = parser.text();
                } else if ("field".equals(fieldName)) {
                    parser.nextToken();
                    field = parser.text();
                } else if ("accuracy".equals(fieldName)) {
                    parser.nextToken();
                    accuracy = parser.floatValue();
                } else if ("num_suggest".equals(fieldName)) {
                    parser.nextToken();
                    numSuggest = parser.intValue();
                } else if ("suggest_mode".equals(fieldName)) {
                    parser.nextToken();
                    String suggestModeVal = parser.text();
                    if ("when_not_in_index".equals(suggestModeVal)) {
                        suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
                    } else if ("more_popular".equals(suggestModeVal)) {
                        suggestMode = SuggestMode.SUGGEST_MORE_POPULAR;
                    } else if ("always".equals(suggestModeVal)) {
                        suggestMode = SuggestMode.SUGGEST_ALWAYS;
                    } else {
                        throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + suggestModeVal);
                    }
                }
            }
        }

        SearchContextSpellCheck searchContextSpellCheck = new SearchContextSpellCheck();
        searchContextSpellCheck.spellCheckAnalyzer(analyzer);
        searchContextSpellCheck.spellCheckText(text);
        if (field == null) {
            throw new ElasticSearchIllegalStateException("Missing spell check field");
        }
        searchContextSpellCheck.setSpellCheckField(field);
        searchContextSpellCheck.accuracy(accuracy);
        searchContextSpellCheck.numSuggest(numSuggest);
        searchContextSpellCheck.suggestMode(suggestMode);
        context.spellcheck(searchContextSpellCheck);
    }
}
