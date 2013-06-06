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

package org.elasticsearch.action.admin.indices.warmer.get;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.state.BaseTransportGetClusterInfoAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class BaseTransportGetWarmersClusterInfoAction extends BaseTransportGetClusterInfoAction<GetWarmersClusterStateInfoRequest, GetWarmersResponse> {

    @Inject
    public BaseTransportGetWarmersClusterInfoAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String transportAction() {
        return GetWarmersAction.NAME;
    }

    @Override
    protected GetWarmersClusterStateInfoRequest newRequest() {
        return new GetWarmersClusterStateInfoRequest();
    }

    @Override
    protected GetWarmersResponse newResponse() {
        return new GetWarmersResponse();
    }

    @Override
    protected GetWarmersResponse doMasterOperation(GetWarmersClusterStateInfoRequest request, ClusterState state) throws ElasticSearchException {
        ImmutableMap<String, ImmutableList<IndexWarmersMetaData.Entry>> result = state.metaData().findWarmers(
                request.indices(), request.types(), request.warmers()
        );
        return new GetWarmersResponse(result);
    }
}
