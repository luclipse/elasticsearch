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

package org.elasticsearch.search.query;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.grouping.AggregatedGroups;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

import static org.elasticsearch.search.dfs.AggregatedDfs.readAggregatedDfs;

/**
 *
 */
public class QuerySearchRequest extends TransportRequest {

    private long id;

    private AggregatedDfs dfs;

    private AggregatedGroups groups;

    public QuerySearchRequest() {
    }

    public QuerySearchRequest(SearchRequest request, long id, AggregatedDfs dfs) {
        super(request);
        this.id = id;
        this.dfs = dfs;
    }

    public QuerySearchRequest(SearchRequest request, long id, AggregatedGroups groups) {
        super(request);
        this.id = id;
        this.groups = groups;
    }

    public long id() {
        return id;
    }

    public AggregatedDfs dfs() {
        return dfs;
    }

    public AggregatedGroups groups() {
        return groups;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
        if (in.readBoolean()) {
            dfs = readAggregatedDfs(in);
        }
        if (in.readBoolean()) {
            groups = new AggregatedGroups();
            groups.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        if (dfs != null) {
            out.writeBoolean(true);
            dfs.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        if (groups != null) {
            out.writeBoolean(true);
            groups.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}
