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

package org.elasticsearch.rest.action.admin.indices.open;

import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

/**
 *
 */
public class RestOpenIndexAction extends BaseRestHandler {

    @Inject
    public RestOpenIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.POST, "/_open", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_open", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        openIndexRequest.listenerThreaded(false);
        openIndexRequest.timeout(request.paramAsTime("timeout", openIndexRequest.timeout()));
        openIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", openIndexRequest.masterNodeTimeout()));
        openIndexRequest.ignoreIndices(IndicesOptions.fromRequest(request, openIndexRequest.ignoreIndices()));
        client.admin().indices().open(openIndexRequest, new AcknowledgedRestResponseActionListener<OpenIndexResponse>(request, channel, logger));
    }
}
