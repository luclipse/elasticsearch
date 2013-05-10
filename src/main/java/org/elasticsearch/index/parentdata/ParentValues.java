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

package org.elasticsearch.index.parentdata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;

/**
 */
public interface ParentValues {

    public static final ParentValues EMPTY = new ParentValues() {
        private final HashedBytesRef EMPTY = new HashedBytesRef(new BytesRef());

        @Override
        public HashedBytesRef parentIdByDoc(int docId) {
            return EMPTY;
        }

        @Override
        public HashedBytesRef idByDoc(int docId) {
            return EMPTY;
        }

        @Override
        public HashedBytesRef makeSafe(HashedBytesRef ref) {
            return ref;
        }
    };

    /**
     * The parent id for the relevant docId. Returns a length of 0 if doesn't exists.
     */
    HashedBytesRef parentIdByDoc(int docId);

    /**
     * The UiD based on the doc id, note, only UIDs relevant to the parent/child relationship
     * are actually loaded. Returns a length of 0 if doesn't exists.
     */
    HashedBytesRef idByDoc(int docId);

    /**
     * Makes a "safe" instance of the bytes ref (for example, to be placed in a Map). Note,
     * this doesn't copy over the bytes if not needed, the assumption is that the objects
     * are only used for reads.
     */
    HashedBytesRef makeSafe(HashedBytesRef ref);
}
