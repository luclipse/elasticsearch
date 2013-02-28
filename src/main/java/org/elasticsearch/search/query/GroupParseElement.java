package org.elasticsearch.search.query;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.grouping.GroupContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.List;

/**
 */
public class GroupParseElement implements SearchParseElement {

    private final SortParseElement sortParseElement = new SortParseElement();

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        String groupField = null;
        Sort sortWithinGroup = Sort.RELEVANCE;
        int sizeWithinGroup = 1;
        int offsetWithinGroup = 0;

        String currentField = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentField = parser.currentName();
                String indexName = parser.currentName();
                if (indexName.equals(context.shardTarget().index())) {
                    parser.nextToken(); // move to the value
                    // we found our query boost
                    context.queryBoost(parser.floatValue());
                }
            } else if (token.isValue()) {
                if ("field".equals(currentField)) {
                    groupField = parser.text();
                } else if ("size".equals(currentField)) {
                    sizeWithinGroup = parser.intValue();
                } else if ("offset".equals(currentField)) {
                    offsetWithinGroup = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("sort_within_group".equals(currentField)) {
                    List<SortField> sortFields = sortParseElement.retrieveSortFields(parser, context);
                    if (!sortFields.isEmpty()) {
                        sortWithinGroup = new Sort(sortFields.toArray(new SortField[sortFields.size()]));
                    }
                }
            }
        }

        if (groupField != null) {
            GroupContext groupContext = new GroupContext();
            groupContext.groupField(groupField);
            groupContext.sortWithinGroup(sortWithinGroup);
            groupContext.sizeWithinGroup(sizeWithinGroup);
            groupContext.offsetWithinGroup(offsetWithinGroup);
            context.group(groupContext);
        }
    }

}
