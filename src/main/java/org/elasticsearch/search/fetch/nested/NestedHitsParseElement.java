package org.elasticsearch.search.fetch.nested;

import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class NestedHitsParseElement implements SearchParseElement {

    private final SortParseElement sortParseElement = new SortParseElement();

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        Map<String, NestedHitsSearchContext.NestedHit> nestedHits = new HashMap<String, NestedHitsSearchContext.NestedHit>(3);
        XContentParser.Token token;
        String topLevelName = null;
        boolean sourceRequired = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String path = null;
                Query childQuery = null;
                Sort innerSort = null;
                int size = 3;
                int offset = 0;
                List<String> fields = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("path".equals(currentFieldName)) {
                            path = parser.text();
                        } else if ("offset".equals(currentFieldName)) {
                            offset = parser.intValue();
                        } else if ("size".equals(currentFieldName)) {
                            size = parser.intValue();
                        } else if ("fields".equals(currentFieldName)) {
                            fields = Lists.newArrayList(parser.text());
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("query".equals(currentFieldName)) {
                            childQuery = context.queryParserService().parseInnerQuery(parser);
                        } else if ("filter".equals(currentFieldName)) {
                            childQuery = new XConstantScoreQuery(context.queryParserService().parseInnerFilter(parser).filter());
                        } else if ("sort".equals(currentFieldName)) {
                            List<SortField> sortFields = sortParseElement.retrieveSortFields(parser, context);
                            if (!sortFields.isEmpty()) {
                                innerSort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));
                            }
                        } else if ("fields".equals(currentFieldName)) {
                            fields = Lists.newArrayList();
                            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                String field = parser.text();
                                if ("_source".equals(field)) {
                                    sourceRequired = true;
                                }
                                fields.add(field);
                            }
                        }
                    }
                }
                if (path == null) {
                    throw new SearchParseException(context, "[path] is a required option");
                }

                MapperService.SmartNameObjectMapper mapper = context.smartNameObjectMapper(path);
                if (mapper == null) {
                    throw new SearchParseException(context, "[nested] failed to find nested object under path [" + path + "]");
                }
                ObjectMapper objectMapper = mapper.mapper();
                if (objectMapper == null) {
                    throw new SearchParseException(context, "[nested] failed to find nested object under path [" + path + "]");
                }
                if (!objectMapper.nested().isNested()) {
                    throw new SearchParseException(context, "[nested] nested object under path [" + path + "] is not of nested type");
                }
                Filter childFilter = context.filterCache().cache(objectMapper.nestedTypeFilter());
                if (childQuery != null) {
                    childQuery = new XFilteredQuery(childQuery, childFilter);
                }

                nestedHits.put(topLevelName, new NestedHitsSearchContext.NestedHit(path, childQuery, childFilter, innerSort, size, offset, fields));
            }
        }
        context.nestedHits(new NestedHitsSearchContext(nestedHits, sourceRequired));
    }
}
