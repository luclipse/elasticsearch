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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * Specifies what type of requested indices to exclude.
 */
public class IndicesOptions {

    private static final IndicesOptions[] IGNORE_INDICES;

    static {
        int max = 1 << 4;
        IGNORE_INDICES = new IndicesOptions[max];
        for (int i = 0; i < max; i++) {
            boolean ignoreUnavailable = (i & 1) != 0;
            boolean allowNoIndices = (i & 2) != 0;
            boolean wildcardExpandToOpen = (i & 4) != 0;
            boolean wildcardExpandToClosed = (i & 8) != 0;
            IGNORE_INDICES[i] = new IndicesOptions(ignoreUnavailable, allowNoIndices, wildcardExpandToOpen, wildcardExpandToClosed);
        }
    }

    private final boolean ignoreUnavailable;
    private final boolean allowNoIndices;
    private final boolean expandWildcardsOpen;
    private final boolean expandWildcardsClosed;

    private IndicesOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandWildcardsOpen, boolean wildcardExpandToClosed) {
        this.ignoreUnavailable = ignoreUnavailable;
        this.allowNoIndices = allowNoIndices;
        this.expandWildcardsOpen = expandWildcardsOpen;
        this.expandWildcardsClosed = wildcardExpandToClosed;
    }

    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }

    public boolean allowNoIndices() {
        return allowNoIndices;
    }

    public boolean expandWildcardsOpen() {
        return expandWildcardsOpen;
    }

    public boolean expandWildcardsClosed() {
        return expandWildcardsClosed;
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        out.write(toByte(ignoreUnavailable, allowNoIndices, expandWildcardsOpen, expandWildcardsClosed));
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        byte id = in.readByte();
        if (id >= IGNORE_INDICES.length) {
            throw new ElasticSearchIllegalArgumentException("No valid missing index type id: " + id);
        }
        return IGNORE_INDICES[id];
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices) {
        byte id = toByte(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices);
        return IGNORE_INDICES[id];
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        boolean expandWildcardsOpen = defaultSettings.expandWildcardsOpen();
        boolean expandWildcardsClosed = defaultSettings.expandWildcardsClosed();

        String[] wildcards = Strings.splitStringByCommaToArray(request.param("expand_wildcards"));
        for (String wildcard : wildcards) {
            if ("open".equals(wildcard)) {
                expandWildcardsOpen = true;
            } else if ("closed".equals(wildcard)) {
                expandWildcardsClosed = true;
            } else {
                throw new ElasticSearchIllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
            }
        }

        return fromOptions(
                request.paramAsBoolean("ignore_unavailable", defaultSettings.ignoreUnavailable()),
                request.paramAsBoolean("allow_no_indices", defaultSettings.allowNoIndices()),
                expandWildcardsOpen,
                expandWildcardsClosed
        );
    }

    /**
     * @return indices options that requires any specified index to exists, expands wildcards only to open indices and
     *         allow that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strict() {
        return IGNORE_INDICES[6];
    }

    /**
     * @return indices options that ignore unavailable indices, expand wildcards only to open indices and
     *         allow that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenient() {
        return IGNORE_INDICES[7];
    }

    private static byte toByte(boolean ignoreUnavailable, boolean allowNoIndices, boolean wildcardExpandToOpen, boolean wildcardExpandToClosed) {
        byte id = 0;
        if (ignoreUnavailable) {
            id += 1;
        }
        if (allowNoIndices) {
            id += 2;
        }
        if (wildcardExpandToOpen) {
            id += 4;
        }
        if (wildcardExpandToClosed) {
            id += 8;
        }
        return id;
    }

}
