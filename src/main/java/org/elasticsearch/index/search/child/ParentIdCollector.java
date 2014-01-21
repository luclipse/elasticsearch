/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;

import java.io.IOException;

/**
 * A simple collector that only collects if the docs parent ID is not
 * <code>null</code>
 */
abstract class ParentIdCollector extends NoopCollector {

    protected final String parentType;
    private final ParentChildIndexFieldData indexFieldData;

    private BytesValues values;

    protected ParentIdCollector(String parentType, ParentChildIndexFieldData indexFieldData) {
        this.parentType = parentType;
        this.indexFieldData = indexFieldData;
    }

    @Override
    public final void collect(int doc) throws IOException {
        if (values != null) {
            int numValues = values.setDocument(doc);
            if (numValues == 1) {
                BytesRef parentId = values.nextValue();
                int hash = values.currentValueHash();
                collect(doc, parentId, hash);
            } else {
                assert numValues == 0;
            }
        }
    }
    
    protected abstract void collect(int doc, BytesRef parentId, int hash) throws IOException;

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getBytesValues(parentType);
    }
}
