/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.parentordinals;

import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 */
public class ParentOrdinals {

    private final PackedInts.Reader ordinals;

    private ParentOrdinals(PackedInts.Reader ordinals) {
        this.ordinals = ordinals;
    }

    public int ordinal(int doc) {
        return (int) ordinals.get(doc);
    }

    public boolean isEmpty() {
        return ordinals.getBitsPerValue() == 0;
    }

    public void forEachOrdinal(OnOrdinal onOrdinal) {
        for (int docId = 0; docId < ordinals.size(); docId++) {
            onOrdinal.onOrdinal((int) ordinals.get(docId));
        }
    }

    public static class Builder {

        private final int maxDoc;
        private GrowableWriter ordinals;

        public Builder(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        public void set(int doc, int ordinal) {
            if (ordinals == null) {
                ordinals = new GrowableWriter(1, maxDoc, PackedInts.FAST);
            }
            this.ordinals.set(doc, ordinal);
        }

        public ParentOrdinals build() {
            return new ParentOrdinals(ordinals != null ? ordinals.getMutable() : new PackedInts.NullReader(maxDoc));
        }

    }

    public interface OnOrdinal {

        public void onOrdinal(int ordinal);

    }

}
