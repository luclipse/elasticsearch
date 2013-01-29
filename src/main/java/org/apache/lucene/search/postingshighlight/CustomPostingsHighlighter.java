/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.lucene.search.postingshighlight;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.List;

/**
 */
public class CustomPostingsHighlighter extends XPostingsHighlighter {

    private final FieldMapper mapper;
    private final SearchContext searchContext;
    private final int maxLength;
    private final CustomPassageFormatter formatter;

    private FetchSubPhase.HitContext hitContext;
    private Query rewrittenQuery;

    public CustomPostingsHighlighter(int maxLength, BreakIterator breakIterator, PassageScorer scorer,
                                     CustomPassageFormatter formatter, FieldMapper mapper,
                                     SearchContext searchContext, boolean fieldMatch) {
        super(maxLength, breakIterator, scorer, formatter, fieldMatch);
        this.maxLength = maxLength;
        this.mapper = mapper;
        this.searchContext = searchContext;
        this.formatter = formatter;
    }

    @Override
    protected Query rewrite(Query original) throws IOException {
        if (rewrittenQuery != null) {
            return rewrittenQuery;
        }

        if (original instanceof MultiTermQuery) {
            MultiTermQuery copy = (MultiTermQuery) original.clone();
            copy.setRewriteMethod(new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(1024));
            original = copy;
        }

        IndexReader reader = hitContext.topLevelReader();
        Query query = original;
        for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query; rewrittenQuery = query.rewrite(reader)) {
            query = rewrittenQuery;
        }
        return rewrittenQuery = query;
    }

    @Override
    protected String[][][] fetchStoredData(String[] fields, int[] docids, IndexSearcher searcher) throws IOException {
        String[][][] content = new String[fields.length][docids.length][];
        for (int i = 0; i < docids.length; i++) {
            int docid = docids[i];
            for (int j = 0; j < fields.length; j++) {
                String field = fields[j];
                if (mapper.fieldType().stored()) {
                    CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(ImmutableSet.of(field), false);
                    hitContext.reader().document(docid, fieldVisitor);
                    List<Object> rawValues = fieldVisitor.fields().get(mapper.names().indexName());
                    String[] values = new String[rawValues.size()];
                    for (int k = 0; k < rawValues.size(); k++) {
                        values[k] = String.valueOf(rawValues.get(k));
                        if (values[k].length() > maxLength) {
                            values[k] = values[k].substring(0, maxLength);
                        }
                    }
                    content[j][i] = values;
                } else {
                    SearchLookup lookup = searchContext.lookup();
                    lookup.setNextReader(hitContext.readerContext());
                    lookup.setNextDocId(docid);
                    List<Object> rawValues = lookup.source().extractRawValues(mapper.names().sourcePath());
                    String[] values = new String[rawValues.size()];
                    for (int k = 0; k < rawValues.size(); k++) {
                        values[k] = String.valueOf(rawValues.get(k));
                        if (values[k].length() > maxLength) {
                            values[k] = values[k].substring(0, maxLength);
                        }
                    }
                    content[j][i] = values;
                }
            }
        }
        return content;
    }

    public void setHitContext(FetchSubPhase.HitContext hitContext) {
        this.hitContext = hitContext;
    }

    public CustomPassageFormatter getFormatter() {
        return formatter;
    }

    public static TopDocs topDocs(int docId) {
        return new TopDocs(1, new ScoreDoc[]{new ScoreDoc(docId, -1)}, -1);
    }

}
