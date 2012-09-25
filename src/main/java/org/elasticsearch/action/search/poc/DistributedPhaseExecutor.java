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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Martijn van Groningen
 */
public class DistributedPhaseExecutor {

    private final List<DistributedPhase> phases = new ArrayList<DistributedPhase>();

    public void prepare(SearchRequest request, DistributedContext context) {
        switch (request.searchType()) {
            case DFS_QUERY_THEN_FETCH:
                break;
            default:
                throw new ElasticSearchException("Can't construct distributed execution path");
        }
    }

    public void process(DistributedContext context, SearchRequest request, ActionListener<SearchResponse> listener) {
        Iterator<DistributedPhase> phaseIterator = phases.iterator();
        innerProcess(context, request, phaseIterator, listener);
    }

    void innerProcess(final DistributedContext context,
                      final SearchRequest request,
                      final Iterator<DistributedPhase> phaseIterator,
                      final ActionListener<SearchResponse> listener) {
        if (!phaseIterator.hasNext()) {
            SearchResponse response = new SearchResponse();
            for (DistributedPhase phase : phases) {
                for (DistributedAction action : phase.getActions()) {
                    action.enrich(response);
                }
            }
            listener.onResponse(response);
            return;
        }

        DistributedPhase phase = phaseIterator.next();
        Iterator<DistributedAction> actionIterator = phase.getActions().iterator();
        DistributedActionListener distributedListener = new DistributedActionListener() {
            @Override
            public void onFinish(Object response) {
                innerProcess(context, request, phaseIterator, listener);
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        };
        while (actionIterator.hasNext()) {
            DistributedAction action = actionIterator.next();
            Tuple<ActionListener, List<Object>> requests = action.prepare(context, request, distributedListener);
            for (Object o : requests.v2()) {
                action.getShardOperation().execute(o, context, requests.v1());
            }
        }
    }

}
