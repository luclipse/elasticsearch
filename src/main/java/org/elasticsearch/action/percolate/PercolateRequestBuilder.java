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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 *
 */
public class PercolateRequestBuilder extends BroadcastOperationRequestBuilder<PercolateRequest, PercolateResponse, PercolateRequestBuilder> {

    public PercolateRequestBuilder(Client client, String index, String type) {
        super((InternalClient) client, new PercolateRequest(index, type));
    }

    PercolateRequestBuilder(Client client) {
        super((InternalClient) client, new PercolateRequest());
    }

    /**
     * Sets the index to percolate the document against.
     */
    public PercolateRequestBuilder setIndex(String index) {
        request.indices(index);
        return this;
    }

    /**
     * Sets the index the document belongs to.
     */
    public PercolateRequestBuilder setDocumentIndex(String index) {
        request.documentIndex(index);
        return this;
    }

    /**
     * Sets the type of the document to percolate.
     */
    public PercolateRequestBuilder setDocumentType(String type) {
        request.documentType(type);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public PercolateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * List of routing values to control the shards the search will be executed on.
     */
    public PercolateRequestBuilder setRouting(String... routings) {
        request.routing(Strings.arrayToCommaDelimitedString(routings));
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public PercolateRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     */
    public PercolateRequestBuilder setSource(Map<String, Object> source) {
        request.documentSource(source);
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public PercolateRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.documentSource(source, contentType);
        return this;
    }

    /**
     * Sets the document source to index.
     * <p/>
     * <p>Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[])}.
     */
    public PercolateRequestBuilder setSource(String source) {
        request.documentSource(source);
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public PercolateRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.documentSource(sourceBuilder);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PercolateRequestBuilder setSource(BytesReference source) {
        request.documentSource(source, false);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PercolateRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.documentSource(source, unsafe);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PercolateRequestBuilder setSource(byte[] source) {
        request.documentSource(source);
        return this;
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public PercolateRequestBuilder setSource(byte[] source, int offset, int length) {
        request.documentSource(source, offset, length);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @param unsafe Is the byte array safe to be used form a different thread
     */
    public PercolateRequestBuilder setSource(byte[] source, int offset, int length, boolean unsafe) {
        request.documentSource(source, offset, length, unsafe);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PercolateResponse> listener) {
        ((Client) client).percolate(request, listener);
    }

}
