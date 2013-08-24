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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.highlight.HighlightField;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class PercolateResponse extends BroadcastOperationResponse implements Iterable<PercolateResponse.Match>, ToXContent {

    private static final Match[] EMPTY = new Match[0];

    private long tookInMillis;
    private Match[] matches;
    private long count;

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures,
                             Match[] matches, long count, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = matches;
        this.count = count;
    }

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, long count, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = EMPTY;
        this.count = count;
    }

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = EMPTY;
    }

    PercolateResponse() {
    }

    public PercolateResponse(Match[] matches) {
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

    public Match[] getMatches() {
        return this.matches;
    }

    public long getCount() {
        return count;
    }

    @Override
    public Iterator<Match> iterator() {
        return Arrays.asList(matches).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(Fields.TOOK, tookInMillis);
        RestActions.buildBroadcastShardsHeader(builder, this);

        builder.field(Fields.TOTAL, count);
        if (matches.length != 0) {
            builder.startArray(Fields.MATCHES);
            boolean justIds = "ids".equals(params.param("percolate_format"));
            if (justIds) {
                for (PercolateResponse.Match match : matches) {
                    builder.value(match.id());
                }
            } else {
                for (PercolateResponse.Match match : matches) {
                    builder.startObject();
                    builder.field(Fields._INDEX, match.getIndex());
                    builder.field(Fields._ID, match.getId());
                    float score = match.score();
                    if (score != PercolatorService.NO_SCORE) {
                        builder.field(Fields._SCORE, match.getScore());
                    }
                    if (match.getHl() != null) {
                        builder.startObject(Fields.HIGHLIGHT);
                        for (HighlightField field : match.getHl().values()) {
                            builder.field(field.name());
                            if (field.fragments() == null) {
                                builder.nullValue();
                            } else {
                                builder.startArray();
                                for (Text fragment : field.fragments()) {
                                    builder.value(fragment);
                                }
                                builder.endArray();
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        count = in.readVLong();
        int size = in.readVInt();
        matches = new Match[size];
        for (int i = 0; i < size; i++) {
            matches[i] = new Match();
            matches[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeVLong(count);
        out.writeVInt(matches.length);
        for (Match match : matches) {
            match.writeTo(out);
        }
    }

    public static class Match implements Streamable {

        private Text index;
        private Text id;
        private float score;
        private Map<String, HighlightField> hl;

        public Match(Text index, Text id, float score, Map<String, HighlightField> hl) {
            this.id = id;
            this.score = score;
            this.index = index;
            this.hl = hl;
        }

        public Match(Text index, Text id, float score) {
            this.id = id;
            this.score = score;
            this.index = index;
        }

        Match() {
        }

        public Text index() {
            return index;
        }

        public Text id() {
            return id;
        }

        public float score() {
            return score;
        }

        public Text getIndex() {
            return index();
        }

        public Map<String, HighlightField> hl() {
            return hl;
        }

        public Text getId() {
            return id();
        }

        public float getScore() {
            return score();
        }

        public Map<String, HighlightField> getHl() {
            return hl;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readText();
            index = in.readText();
            score = in.readFloat();
            int size = in.readVInt();
            if (size > 0) {
                hl = new HashMap<String, HighlightField>(size);
                for (int j = 0; j < size; j++) {
                    hl.put(in.readString(), HighlightField.readHighlightField(in));
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeText(id);
            out.writeText(index);
            out.writeFloat(score);
            if (hl != null) {
                out.writeVInt(hl.size());
                for (Map.Entry<String, HighlightField> entry : hl.entrySet()) {
                    out.writeString(entry.getKey());
                    entry.getValue().writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString HIGHLIGHT = new XContentBuilderString("highlight");
    }

}
