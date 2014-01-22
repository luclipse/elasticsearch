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
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;

import java.io.IOException;

/**
 * A simple collector that only collects if the docs parent ID is not
 * <code>null</code>
 */
abstract class ParentIdCollector extends NoopCollector {

    protected final String parentType;
    private final ParentChildIndexFieldData indexFieldData;
    private final HashedBytesRef spare = new HashedBytesRef();

    protected BytesValues.WithOrdinals values;
    private Ordinals.Docs ordinals;

    protected ParentIdCollector(String parentType, ParentChildIndexFieldData indexFieldData) {
        this.parentType = parentType;
        this.indexFieldData = indexFieldData;
    }

    @Override
    public final void collect(int doc) throws IOException {
        if (values != null) {
            long ord = ordinals.getOrd(doc);
            spare.bytes = values.getValueByOrd(ord);
            spare.hash = values.currentValueHash();
            collect(spare);
        }
    }
    
    protected abstract void collect(HashedBytesRef spare) throws IOException;

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getBytesValues(parentType);
        ordinals = values.ordinals();
    }
}
