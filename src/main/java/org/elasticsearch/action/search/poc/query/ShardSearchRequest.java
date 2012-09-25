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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search.poc.query;

import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;

import java.io.IOException;

/**
 * @author Martijn van Groningen
 */
public class ShardSearchRequest implements Streamable {

    private ShardRouting shardRouting;
    private QuerySearchRequest request1;
    private InternalSearchRequest request2;

    public QuerySearchRequest getRequest1() {
        return request1;
    }

    public void setRequest1(QuerySearchRequest request1) {
        this.request1 = request1;
        request2 = null;
    }

    public InternalSearchRequest getRequest2() {
        return request2;
    }

    public void setRequest2(InternalSearchRequest request2) {
        request1 = null;
        this.request2 = request2;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public void setShardRouting(ShardRouting shardRouting) {
        this.shardRouting = shardRouting;
    }
    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            request1 = create1(in);
        } else {
            request2 = create2(in);
        }
        shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(request1 != null);
        if (request1 != null) {
            request1.writeTo(out);
        } else {
            request2.writeTo(out);
        }
        shardRouting.writeTo(out);
    }

    public static QuerySearchRequest create1(StreamInput input) throws IOException {
        QuerySearchRequest request = new QuerySearchRequest();
        request.readFrom(input);
        return request;
    }

    public static InternalSearchRequest create2(StreamInput input) throws IOException {
        InternalSearchRequest request = new InternalSearchRequest();
        request.readFrom(input);
        return request;
    }
}
