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

package org.elasticsearch.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.*;

/**
 *
 */
public class QuerySearchResult extends TransportResponse implements QuerySearchResultProvider {

    private long id;
    private SearchShardTarget shardTarget;
    private int from;
    private int size;
    private TopDocs topDocs;
    private InternalFacets facets;
    private Suggest suggest;
    private boolean searchTimedOut;

    private TopGroups topGroups;
    private Sort sortWithinGroup;
    private Sort groupSort;
    private int offsetWithinGroup;
    private int sizeWithinGroup;

    public QuerySearchResult() {

    }

    public QuerySearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    @Override
    public boolean includeFetch() {
        return false;
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
        for (GroupDocs group : topGroups.groups) {
            for (ScoreDoc scoreDoc : group.scoreDocs) {
                scoreDoc.shardIndex = shardTarget.shardId();
            }
        }
    }

    public void searchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public TopDocs topDocs() {
        return topDocs;
    }

    public void topDocs(TopDocs topDocs) {
        this.topDocs = topDocs;
    }

    public void topGroups(TopGroups topGroups) {
        this.topGroups = topGroups;
    }

    public TopGroups topGroups() {
        return topGroups;
    }

    public Facets facets() {
        return facets;
    }

    public void facets(InternalFacets facets) {
        this.facets = facets;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public int from() {
        return from;
    }

    public QuerySearchResult from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public QuerySearchResult size(int size) {
        this.size = size;
        return this;
    }

    public Sort sortWithinGroup() {
        return sortWithinGroup;
    }

    public void sortWithinGroup(Sort sortWithinGroup) {
        this.sortWithinGroup = sortWithinGroup;
    }

    public Sort groupSort() {
        return groupSort;
    }

    public void groupSort(Sort groupSort) {
        this.groupSort = groupSort;
    }

    public int offsetWithinGroup() {
        return offsetWithinGroup;
    }

    public void offsetWithinGroup(int offsetWithinGroup) {
        this.offsetWithinGroup = offsetWithinGroup;
    }

    public int sizeWithinGroup() {
        return sizeWithinGroup;
    }

    public void sizeWithinGroup(int sizeWithinGroup) {
        this.sizeWithinGroup = sizeWithinGroup;
    }

    public static QuerySearchResult readQuerySearchResult(StreamInput in) throws IOException {
        QuerySearchResult result = new QuerySearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        from = in.readVInt();
        size = in.readVInt();
        if (in.readBoolean()) {
            topDocs = readTopDocs(in);
        }
        if (in.readBoolean()) {
            topGroups = readTopGroups(in);
        }
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(in);
        }
        searchTimedOut = in.readBoolean();

        sortWithinGroup = readSort(in);
        groupSort = readSort(in);
        offsetWithinGroup = in.readVInt();
        sizeWithinGroup = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
//        shardTarget.writeTo(out);
        out.writeVInt(from);
        out.writeVInt(size);
        if (topDocs != null) {
            out.writeBoolean(true);
            writeTopDocs(out, topDocs, 0);
        } else {
            out.writeBoolean(false);
        }

        if (topGroups != null) {
            out.writeBoolean(true);
            writeTopGroups(topGroups, out);
        } else {
            out.writeBoolean(false);
        }

        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }

        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);

        writeSort(sortWithinGroup, out);
        writeSort(groupSort, out);
        out.writeVInt(offsetWithinGroup);
        out.writeVInt(sizeWithinGroup);
    }
}
