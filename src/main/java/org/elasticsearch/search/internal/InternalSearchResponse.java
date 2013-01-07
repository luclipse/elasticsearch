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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.spellcheck.SpellCheckResult;

import java.io.IOException;

import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

/**
 *
 */
public class InternalSearchResponse implements Streamable, ToXContent {

    private InternalSearchHits hits;

    private InternalFacets facets;

    private SpellCheckResult spellCheck;

    private boolean timedOut;

    public static final InternalSearchResponse EMPTY = new InternalSearchResponse(new InternalSearchHits(new InternalSearchHit[0], 0, 0), null, null, false);

    private InternalSearchResponse() {
    }

    public InternalSearchResponse(InternalSearchHits hits, InternalFacets facets, SpellCheckResult spellcheck, boolean timedOut) {
        this.hits = hits;
        this.facets = facets;
        this.spellCheck = spellcheck;
        this.timedOut = timedOut;
    }

    public boolean timedOut() {
        return this.timedOut;
    }

    public SearchHits hits() {
        return hits;
    }

    public Facets facets() {
        return facets;
    }

    public SpellCheckResult spellCheck() {
        return spellCheck;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (facets != null) {
            facets.toXContent(builder, params);
        }
        return builder;
    }

    public static InternalSearchResponse readInternalSearchResponse(StreamInput in) throws IOException {
        InternalSearchResponse response = new InternalSearchResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        hits = readSearchHits(in);
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            spellCheck = SpellCheckResult.readSpellCheck(in);
        }
        timedOut = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }
        if (spellCheck == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            spellCheck.writeTo(out);
        }
        out.writeBoolean(timedOut);
    }
}
