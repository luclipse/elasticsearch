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

package org.elasticsearch.indices.warmer;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public interface IndicesWarmer {

    public abstract class Listener {

        public String executor() {
            return ThreadPool.Names.WARMER;
        }

        public abstract void warm(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool);
    }

    public static class WarmerContext {

        private final ShardId shardId;

        private final Engine.Searcher newSearcher;

        private final Engine.Searcher completeSearcher;

        public WarmerContext(ShardId shardId, Engine.Searcher newSearcher, Engine.Searcher completeSearcher) {
            this.shardId = shardId;
            this.newSearcher = newSearcher;
            this.completeSearcher = completeSearcher;
        }

        public WarmerContext(ShardId shardId, Engine.Searcher searcher) {
            this.shardId = shardId;
            this.newSearcher = searcher;
            this.completeSearcher = searcher;
        }

        public ShardId shardId() {
            return shardId;
        }

        /** Return a searcher instance that only wraps the segments to warm. */
        public Engine.Searcher newSearcher() {
            return newSearcher;
        }

        /**
         * Return a searcher instance that wraps the all segments, including previously warmed segments.
         * Can return <code>null</code> if all the segments aren't available for warming (for example warming during a merge)
         */
        public Engine.Searcher completeSearcher() {
            return completeSearcher;
        }

    }

    void addListener(Listener listener);

    void removeListener(Listener listener);
}
