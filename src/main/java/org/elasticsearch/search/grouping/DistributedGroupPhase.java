package org.elasticsearch.search.grouping;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.search.grouping.XTermFirstPassGroupingCollector;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 */
public class DistributedGroupPhase implements SearchPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        GroupContext groupContext = context.group();
        if (groupContext == null) {
            throw new ElasticSearchIllegalStateException("Executing distributed grouping, but no grouping is specified");
        }

        try {
            if (!context.queryRewritten()) {
                context.updateRewriteQuery(context.searcher().rewrite(context.query()));
            }

            Sort groupSort = context.sort();
            if (groupSort == null) {
                groupSort = Sort.RELEVANCE;
            }

            int topNGroups = context.from() + context.size();
            if (topNGroups == 0) {
                topNGroups = 1;
            }

            FieldMapper groupField = context.mapperService().smartNameFieldMapper(groupContext.groupField());
            IndexFieldData.WithOrdinals groupFieldData = context.fieldData().getForField(groupField);
            XTermFirstPassGroupingCollector firstPassCollector = new XTermFirstPassGroupingCollector(groupSort, topNGroups, groupFieldData);
            context.searcher().search(context.query(), firstPassCollector);
            Collection<SearchGroup<BytesRef>> searchGroups = firstPassCollector.getTopGroups(context.from(), true);
            context.groupResult().searchGroups((Collection) searchGroups);
            context.groupResult().groupSort(groupSort);
            context.groupResult().groupSize(context.size());
            context.groupResult().groupOffset(context.from());
        } catch (IOException e) {
            throw new ElasticSearchException("Error while during distributed first pass grouping", e);
        }
    }
}
