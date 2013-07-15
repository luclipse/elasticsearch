/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class PercolateResponse extends BroadcastOperationResponse implements Iterable<String> {

    private long tookInMillis;
    private String[] matches;

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, String[] matches, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = matches;
    }

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
    }

    PercolateResponse() {
    }

    public PercolateResponse(String[] matches) {
        this.matches = matches;
    }

    /**
     * How long the percolate took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the percolate took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    public String[] getMatches() {
        return this.matches;
    }

    @Override
    public Iterator<String> iterator() {
        return Arrays.asList(matches).iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        matches = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeStringArray(matches);
    }
}
