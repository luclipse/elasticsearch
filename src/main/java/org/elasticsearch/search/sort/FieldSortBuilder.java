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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A sort builder to sort based on a document field.
 */
public class FieldSortBuilder extends SortBuilder {

    private final String fieldName;

    private SortOrder order;

    private Object missing;

    private Boolean ignoreUnampped;

    private String sortMode;

    /**
     * Constructs a new sort based on a document field.
     *
     * @param fieldName The field name.
     */
    public FieldSortBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * The order of sorting. Defaults to {@link SortOrder#ASC}.
     */
    @Override
    public FieldSortBuilder order(SortOrder order) {
        this.order = order;
        return this;
    }

    /**
     * Sets the value when a field is missing in a doc. Can also be set to <tt>_last</tt> or
     * <tt>_first</tt> to sort missing last or first respectively.
     */
    @Override
    public FieldSortBuilder missing(Object missing) {
        this.missing = missing;
        return this;
    }

    /**
     * Sets if the field does not exists in the index, it should be ignored and not sorted by or not. Defaults
     * to <tt>false</tt> (not ignoring).
     */
    public FieldSortBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnampped = ignoreUnmapped;
        return this;
    }

    /**
     * Defines what values to pick in the case a document contains multiple values for the targeted sort field.
     * Possible values: min, max, sum and avg
     * <p/>
     * The last two values are only applicable for number based fields.
     */
    public FieldSortBuilder sortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(fieldName);
        if (order != null) {
            builder.field("order", order.toString());
        }
        if (missing != null) {
            builder.field("missing", missing);
        }
        if (ignoreUnampped != null) {
            builder.field("ignore_unmapped", ignoreUnampped);
        }
        if (sortMode != null) {
            builder.field("sort_mode", sortMode);
        }
        builder.endObject();
        return builder;
    }
}
