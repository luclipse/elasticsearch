package org.elasticsearch.search.fetch.nested;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.nested.NestedDocsFilter;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class NestedHitsFetchSubPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("nested_hits", new NestedHitsParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.nestedHits() != null;
    }

    @Override
    public void hitExecute(SearchContext searchContext, HitContext hitContext) throws ElasticsearchException {
        BytesReference rootSource = hitContext.hit().isSourceEmpty() ? null : hitContext.hit().getSourceRef();
        if (rootSource == null && searchContext.nestedHits().sourceRequired()) {
            JustSourceFieldsVisitor fieldsVisitor = new JustSourceFieldsVisitor();
            try {
                hitContext.reader().document(hitContext.docId(), fieldsVisitor);
            } catch (IOException e) {
                throw new ElasticsearchException("", e);
            }
            rootSource = fieldsVisitor.source();
        }

        Map<String, NestedSearchHits> result = new HashMap<String, NestedSearchHits>();
        for (Map.Entry<String, NestedHitsSearchContext.NestedHit> entry : searchContext.nestedHits().nestedHits().entrySet()) {
            NestedHitsSearchContext.NestedHit nestedHit = entry.getValue();
            try {
                // This also includes the rootDoc!
                Filter childDocsFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUid(hitContext.hit().type(), hitContext.hit().id())));
                // But now we exclude the rootDoc.
                childDocsFilter = new AndFilter(ImmutableList.of(childDocsFilter, NestedDocsFilter.INSTANCE));

                // We can optimize here, we only need to search the child docs from one segment.
                // But the df must be based on the main searcher.
                TopDocs topDocs;
                int topN = nestedHit.offset() + nestedHit.size();
                Query query = nestedHit.childQuery() != null ? nestedHit.childQuery() : searchContext.nestedQueries(nestedHit.path()).getChildQuery();
                Sort sort = nestedHit.innerSort() == null ? searchContext.sort() : nestedHit.innerSort();
                if (nestedHit.innerSort() != null) {
                    topDocs = searchContext.searcher().search(query, childDocsFilter, topN, sort, searchContext.trackScores(), searchContext.trackScores());
                } else  {
                    topDocs = searchContext.searcher().search(query, childDocsFilter, topN);
                }
                NestedSearchHit[] hits = retrieveNestedHits(hitContext, searchContext, rootSource, topDocs, nestedHit);
                if (hits != null) {
                    result.put(entry.getKey(), new NestedSearchHits(topDocs.totalHits, topDocs.getMaxScore(), hits));
                }
            } catch (IOException e) {
                throw new ElasticsearchException("", e);
            }
        }
        hitContext.hit().setNestedHits(result);
    }

    // During indexing the order of the nested docs is reversed. So the the first index in the values array is meant for the highest child document
    private NestedSearchHit[] retrieveNestedHits(HitContext hitContext, SearchContext searchContext, BytesReference rootSource, TopDocs nestedHits, NestedHitsSearchContext.NestedHit nestedHit) throws IOException {
        // TODO: streaming parsing
//        XContentParser parser = XContentHelper.createParser(hitSource);
//        XContentParser.Token token = parser.nextToken();
        if (nestedHits.scoreDocs.length - nestedHit.offset() < 1) {
            return new NestedSearchHit[0];
        }

        NestedSearchHit[] result = new NestedSearchHit[nestedHits.scoreDocs.length];
        FixedBitSet allChildDocs = (FixedBitSet) nestedHit.childFilter().getDocIdSet(hitContext.readerContext(), null);
        int lastChildDoc = allChildDocs.prevSetBit(hitContext.docId());
        if (rootSource != null) {
            Map<String, Object> rootSourceAsMap = XContentHelper.convertToMap(rootSource, true).v2();
            List<Map> nestedSources = (List<Map>) XContentMapValues.extractValue(nestedHit.path(), rootSourceAsMap);
            for (int i = nestedHit.offset(); i < nestedHits.scoreDocs.length; i++) {
                int offset = lastChildDoc - nestedHits.scoreDocs[i].doc;
                BytesReference nestedSource = null;
                if (nestedHit.sourceRequired()) {
                    nestedSource = jsonBuilder().map(nestedSources.get(offset)).bytes();
                }
                Map<String, SearchHitField> nestedHitsHitFields = null;
                if (nestedHit.fields() != null && !nestedHit.fields().isEmpty()) {
                    nestedHitsHitFields = new HashMap<String, SearchHitField>(nestedHit.fields().size());
                    for (String fieldToLoad : nestedHit.fields()) {
                        if (SourceFieldMapper.NAME.equals(fieldToLoad)) {
                            continue;
                        }

                        nestedHitsHitFields.put(fieldToLoad, new InternalSearchHitField(fieldToLoad, XContentMapValues.extractRawValues(fieldToLoad, rootSourceAsMap)));
                    }
                }
                result[i] = new NestedSearchHit(offset, nestedHits.scoreDocs[i].score, nestedSource, nestedHitsHitFields);
            }
        } else {
            boolean loadAllStoredFields = false;
            Set<String> fieldsToLoad = new HashSet<String>();
            for (String fieldToLoad : nestedHit.fields()) {
                if (SourceFieldMapper.NAME.equals(fieldToLoad)) {
                    continue;
                }
                if ("*".equals(fieldToLoad)) {
                    loadAllStoredFields = true;
                }
                fieldsToLoad.add(fieldToLoad);
            }
            FieldsVisitor fieldsVisitor = new CustomFieldsVisitor(fieldsToLoad, loadAllStoredFields);
            for (int i = nestedHit.offset(); i < nestedHits.scoreDocs.length; i++) {
                int offset = lastChildDoc - nestedHits.scoreDocs[i].doc;
                fieldsVisitor.reset();
                try {
                    hitContext.topLevelReader().document(nestedHits.scoreDocs[i].doc, fieldsVisitor);
                } catch (IOException e) {
                    throw new FetchPhaseExecutionException(searchContext, "Failed to fetch doc id [" + nestedHits.scoreDocs[i].doc + "]", e);
                }
                fieldsVisitor.postProcess(searchContext.mapperService());
                Map<String, SearchHitField> searchFields = null;
                if (fieldsVisitor.fields() != null) {
                    searchFields = new HashMap<String, SearchHitField>(fieldsVisitor.fields().size());
                    for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                        searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
                    }
                }
                result[i] = new NestedSearchHit(offset, nestedHits.scoreDocs[i].score, null, searchFields);
            }
        }

        return result;
    }

}
