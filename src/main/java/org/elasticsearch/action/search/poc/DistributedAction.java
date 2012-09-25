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

package org.elasticsearch.action.search.poc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.List;

/**
 * Defines an async distributed action which is stateful.
 */
public interface DistributedAction<ShardRequest, ShardResponse> {

    /**
     * Returns the shards requests to be executed for this action in its current state.
     * Keep invoking this until <code>null</code> is returned.
     */
    Tuple<ActionListener, List<ShardRequest>> prepare(DistributedContext context, SearchRequest request, DistributedActionListener listener);

    ShardOperation<ShardRequest, ShardResponse> getShardOperation();

    /**
     * Handles incoming shard response. If all shard responses are received, then the responses are merged.
     */
    void handleShardResult(ShardResponse shardResponse);

    /**
     * Allows an implementation to add results to the final search response.
     */
    void enrich(IntermediateResult intermediateResult);

}
