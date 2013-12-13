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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * Specifies what type of requested indices to exclude.
 */
public enum IndicesOptions implements Streamable {

    _000(false, false, false),
    _100(true, false, false),
    _010(false, true, false),
    _110(true, true, false),
    _001(false, false, true),
    _101(true, false, true),
    _011(false, true, true),
    _111(true, true, true);

    private static final IndicesOptions[] IGNORE_INDICES = IndicesOptions.values();

    private final boolean ignoreUnavailable;
    private final boolean expandOnlyOpenIndices;
    private final boolean allowNoIndices;

    private IndicesOptions(boolean ignoreUnavailable, boolean expandOnlyOpenIndices, boolean allowNoIndices) {
        this.ignoreUnavailable = ignoreUnavailable;
        this.expandOnlyOpenIndices = expandOnlyOpenIndices;
        this.allowNoIndices = allowNoIndices;
    }

    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }

    public boolean expandOnlyOpenIndices() {
        return expandOnlyOpenIndices;
    }

    public boolean allowNoIndices() {
        return allowNoIndices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new ElasticSearchIllegalStateException("should never be invoked");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.write(id(ignoreUnavailable, allowNoIndices, expandOnlyOpenIndices));
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        byte id = in.readByte();
        if (id >= IGNORE_INDICES.length) {
            throw new ElasticSearchIllegalArgumentException("No valid missing index type id: " + id);
        }
        return IGNORE_INDICES[id];
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean expandOnlyOpenIndices, boolean allowNoIndices) {
        byte id = id(ignoreUnavailable, allowNoIndices, expandOnlyOpenIndices);
        return IGNORE_INDICES[id];
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        return fromOptions(
                request.paramAsBoolean("ignore_unavailable", defaultSettings.ignoreUnavailable()),
                request.paramAsBoolean("expand_wildcards", defaultSettings.expandOnlyOpenIndices()),
                request.paramAsBoolean("allow_no_indices", defaultSettings.allowNoIndices())
        );
    }

    /**
     * @return indices options that requires any specified index to exists, expands wildcards only to open indices  and
     *         allow that no indices are resolved (not returning an error).
     */
    public static IndicesOptions strict() {
        return _011;
    }

    /**
     * @return indices options that ignore unavailable indices, expand wildcards only to open indices and .
     */
    public static IndicesOptions lenient() {
        return _111;
    }

    private static byte id(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandOnlyOpenIndices) {
        byte id = 0;
        if (ignoreUnavailable) {
            id += 1;
        }
        if (allowNoIndices) {
            id += 2;
        }
        if (expandOnlyOpenIndices) {
            id += 4;
        }
        return id;
    }

}
