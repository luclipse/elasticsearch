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
import org.elasticsearch.rest.RestRequest;

/**
 * Specifies what type of requested indices to exclude.
 */
public enum IgnoreIndices {

    _000(false, false, false),
    _100(true, false, false),
    _010(false, true, false),
    _110(true, true, false),
    _001(false, false, true),
    _101(true, false, true),
    _011(true, true, false),
    _111(true, true, true);

    private static final IgnoreIndices[] IGNORE_INDICES = IgnoreIndices.values();

    private final boolean ignoreUnavailable;
    private final boolean expandOnlyOpenIndices;
    private final boolean allowNoIndices;

    private IgnoreIndices(boolean ignoreUnavailable, boolean expandOnlyOpenIndices, boolean allowNoIndices) {
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

    public byte id() {
        byte id = 0;
        if (ignoreUnavailable) {
            id += 1;
        }
        if (expandOnlyOpenIndices) {
            id += 2;
        }
        if (allowNoIndices) {
            id += 4;
        }
        return id;
    }

    public static IgnoreIndices fromId(byte id) {
        if (id >= IGNORE_INDICES.length) {
            throw new ElasticSearchIllegalArgumentException("No valid missing index type id: " + id);
        }
        return IGNORE_INDICES[id];
    }

    public static IgnoreIndices fromOptions(boolean ignoreMissing, boolean expandOnlyOpenIndices, boolean allowNoIndices) {
        byte id = 0;
        if (ignoreMissing) {
            id += 1;
        }
        if (expandOnlyOpenIndices) {
            id += 2;
        }
        if (allowNoIndices) {
            id += 4;
        }
        return IGNORE_INDICES[id];
    }

    public static IgnoreIndices fromRequest(RestRequest request, IgnoreIndices defaultSettings) {
        return fromOptions(
                request.paramAsBoolean("ignore_unavailable", defaultSettings.ignoreUnavailable()),
                request.paramAsBoolean("expand_wildcards", defaultSettings.expandOnlyOpenIndices()),
                request.paramAsBoolean("allow_no_indices", defaultSettings.allowNoIndices())
        );
    }

    public static IgnoreIndices strict() {
        return _010;
    }

    public static IgnoreIndices lenient() {
        return _101;
    }

}
