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

package org.elasticsearch.index.parentdata.paged;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.parentdata.ParentValues;

/**
 */
public class PagedParentValues implements ParentValues {

    private final PagedAtomicParentData.Type type;
    private final HashedBytesRef scratch = new HashedBytesRef(new BytesRef());

    public PagedParentValues(PagedAtomicParentData.Type type) {
        this.type = type;
    }

    @Override
    public HashedBytesRef parentIdByDoc(int docId) {
        long offset = type.docIdToParentIdOffset.get(docId);
        type.parentIds.fill(scratch.bytes, offset);
        return scratch.resetHash(type.hashes[docId]);
    }

    @Override
    public HashedBytesRef idByDoc(int docId) {
        long offset = type.docIdToUidOffset.get(docId);
        type.parentIds.fill(scratch.bytes, offset);
        return scratch.resetHash(type.hashes[docId]);
    }

    @Override
    public HashedBytesRef makeSafe(HashedBytesRef ref) {
        // we only make a shallow copy here, to make sure not to change the internals of the bytes in the hashed one
        // but, since the reader from pages bytes just acts as a "pointer", its fine not to need to copy the bytes
        return new HashedBytesRef(ref.bytes, ref.hash);
    }
}
