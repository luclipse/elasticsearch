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

package org.elasticsearch.common.lucene.codec.bloom;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 *
 */
public interface BloomFilter {

    public static final BloomFilter NONE = new BloomFilter() {

        @Override
        public void add(BytesRef term) {
        }

        @Override
        public boolean isPresent(BytesRef term) {
            return true;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }

        @Override
        public void serialize(DataOutput out) throws IOException {
        }
    };

    public static final BloomFilter EMPTY = new BloomFilter() {

        @Override
        public void add(BytesRef term) {
        }

        @Override
        public boolean isPresent(BytesRef term) {
            return false;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }

        @Override
        public void serialize(DataOutput out) throws IOException {
        }
    };

    void add(BytesRef term);

    boolean isPresent(BytesRef term);

    long sizeInBytes();

    void serialize(DataOutput out) throws IOException;
}