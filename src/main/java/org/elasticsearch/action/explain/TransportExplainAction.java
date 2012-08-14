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

package org.elasticsearch.action.explain;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportShardSingleOperationAction<ExplainRequest, ExplainResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportExplainAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    protected String transportAction() {
        return ExplainAction.NAME;
    }

    protected String executor() {
        return ThreadPool.Names.GET; // Or use Names.SEARCH?
    }

    protected ExplainResponse shardOperation(ExplainRequest request, int shardId) throws ElasticSearchException {
        Engine.Searcher searcher = null;
        try {
            IndexService indexService = indicesService.indexService(request.index());
            IndexShard indexShard = indexService.shardSafe(shardId);
            Term uidTerm = UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(request.type(), request.id()));
            Engine.GetResult result = indexShard.get(new Engine.Get(false, uidTerm));
            if (!result.exists()) {
                return new ExplainResponse(false);
            }

            searcher = result.searcher();
            Query explainQuery = indexService.queryParserService().parse(request.query()).query();
            Filter searchFilter = indexService.mapperService().searchFilter(request.type());
            if (searchFilter != null) {
                explainQuery = new FilteredQuery(explainQuery, indexService.cache().filter().cache(searchFilter));
            }
            int docId = result.docIdAndVersion().docId;
            Explanation explanation = searcher.searcher().explain(explainQuery, docId);
            return new ExplainResponse(true, explanation);
        } catch (IOException e) {
            throw new ElasticSearchException("Could not explain", e);
        } finally {
            if (searcher != null) {
                searcher.release();
            }
        }
    }

    protected ExplainRequest newRequest() {
        return new ExplainRequest();
    }

    protected ExplainResponse newResponse() {
        return new ExplainResponse();
    }

    protected ClusterBlockException checkGlobalBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    protected ShardIterator shards(ClusterState state, ExplainRequest request) throws ElasticSearchException {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing(), request.preference());
    }
}
