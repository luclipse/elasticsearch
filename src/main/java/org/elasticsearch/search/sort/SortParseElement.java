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

package org.elasticsearch.search.sort;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ObjectMappers;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NestedFieldComparatorSource;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;

import static org.elasticsearch.index.search.nested.NestedFieldComparatorSource.NestedOrder;

/**
 *
 */
public class SortParseElement implements SearchParseElement {

    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.Type.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.Type.DOC, true);

    public static final String SCORE_FIELD_NAME = "_score";
    public static final String DOC_FIELD_NAME = "_doc";

    private final ImmutableMap<String, SortParser> parsers;

    public SortParseElement() {
        ImmutableMap.Builder<String, SortParser> builder = ImmutableMap.builder();
        addParser(builder, new ScriptSortParser());
        addParser(builder, new GeoDistanceSortParser());
        this.parsers = builder.build();
    }

    private void addParser(ImmutableMap.Builder<String, SortParser> parsers, SortParser parser) {
        for (String name : parser.names()) {
            parsers.put(name, parser);
        }
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    addCompoundSortField(parser, context, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    addSortField(context, sortFields, parser.text(), false, false, null, NestedOrder.FIRST);
                }
            }
        } else {
            addCompoundSortField(parser, context, sortFields);
        }
        if (!sortFields.isEmpty()) {
            // optimize if we just sort on score non reversed, we don't really need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && !sortField.getReverse()) {
                    sort = false;
                } else {
                    sort = true;
                }
            }
            if (sort) {
                context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
            }
        }
    }

    private void addCompoundSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws Exception {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                boolean reverse = false;
                String missing = null;
                String innerJsonName = null;
                boolean ignoreUnmapped = false;
                NestedOrder nestedOrder = NestedOrder.FIRST;
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    String direction = parser.text();
                    if (direction.equals("asc")) {
                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                    } else if (direction.equals("desc")) {
                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                    }
                    addSortField(context, sortFields, fieldName, reverse, ignoreUnmapped, missing, nestedOrder);
                } else {
                    if (parsers.containsKey(fieldName)) {
                        sortFields.add(parsers.get(fieldName).parse(parser, context));
                    } else {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerJsonName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("reverse".equals(innerJsonName)) {
                                    reverse = parser.booleanValue();
                                } else if ("order".equals(innerJsonName)) {
                                    if ("asc".equals(parser.text())) {
                                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                                    } else if ("desc".equals(parser.text())) {
                                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                                    }
                                } else if ("missing".equals(innerJsonName)) {
                                    missing = parser.textOrNull();
                                } else if ("ignore_unmapped".equals(innerJsonName) || "ignoreUnmapped".equals(innerJsonName)) {
                                    ignoreUnmapped = parser.booleanValue();
                                } else if ("nested_order".equals(innerJsonName)) {
                                    nestedOrder = NestedOrder.parse(parser.text());
                                }
                            }
                        }
                        addSortField(context, sortFields, fieldName, reverse, ignoreUnmapped, missing, nestedOrder);
                    }
                }
            }
        }
    }

    private static void addSortField(SearchContext context, List<SortField> sortFields, String fieldName, boolean reverse, boolean ignoreUnmapped, @Nullable final String missing, NestedOrder nestedOrder) {
        if (SCORE_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_SCORE_REVERSE);
            } else {
                sortFields.add(SORT_SCORE);
            }
        } else if (DOC_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_DOC_REVERSE);
            } else {
                sortFields.add(SORT_DOC);
            }
        } else {
            FieldMapper fieldMapper = context.smartNameFieldMapper(fieldName);
            if (fieldMapper == null) {
                if (ignoreUnmapped) {
                    return;
                }
                throw new SearchParseException(context, "No mapping found for [" + fieldName + "] in order to sort on");
            }

            FieldComparatorSource fieldComparatorSource = context.fieldData().getForField(fieldMapper).comparatorSource(missing);
            ObjectMapper objectMapper = resolveNestedObjectMapper(fieldName, context);
            if (objectMapper != null) {
                Filter parentFilter = context.filterCache().cache(NonNestedDocsFilter.INSTANCE);
                Filter childFilter = context.filterCache().cache(objectMapper.nestedTypeFilter());
                fieldComparatorSource = new NestedFieldComparatorSource(fieldComparatorSource, nestedOrder, parentFilter, childFilter);
            }

            sortFields.add(new SortField(fieldMapper.names().indexName(), fieldComparatorSource, reverse));
        }
    }

    private static ObjectMapper resolveNestedObjectMapper(String fieldName, SearchContext context) {
        int indexOf = fieldName.lastIndexOf('.');
        if (indexOf == -1) {
            return null;
        }

        String objectPath = fieldName.substring(0, indexOf);
        ObjectMappers objectMappers = context.mapperService().objectMapper(objectPath);
        if (objectMappers == null) {
            return null;
        }

        for (ObjectMapper objectMapper : objectMappers) {
            if (objectMapper.nested().isNested()) {
                return objectMapper;
            }
        }

        return null;
    }

}
