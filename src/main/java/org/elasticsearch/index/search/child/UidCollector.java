package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.parentdata.ParentValues;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public abstract class UidCollector extends NoopCollector {

    protected final String parentType;
    protected final SearchContext context;
    protected ParentValues parentValues;

    protected UidCollector(String parentType, SearchContext context) {
        this.parentType = parentType;
        this.context = context;
    }

    @Override
    public final void collect(int doc) throws IOException {
        if (parentValues == ParentValues.EMPTY) {
            // There could be segments that have docs that are neither parent or child doc
            return;
        }
        HashedBytesRef uidByDoc = parentValues.idByDoc(doc);
        collect(doc, uidByDoc); // No need for no value checking, all docs have an uid.
    }

    protected abstract void collect(int doc, HashedBytesRef uid) throws IOException;

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        parentValues = context.parentData().atomic(readerContext.reader()).getValues(parentType);
    }
}
