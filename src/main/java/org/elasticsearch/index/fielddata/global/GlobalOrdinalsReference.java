package org.elasticsearch.index.fielddata.global;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public class GlobalOrdinalsReference implements GlobalReference {

    private final ImmutableOpenMap<Object, Ordinals> ordinalsLookup;

    public GlobalOrdinalsReference(ImmutableOpenMap<Object, Ordinals> ordinalsLookup) {
        this.ordinalsLookup = ordinalsLookup;
    }

    @Override
    public Ordinals ordinals(AtomicReaderContext context) {
        return ordinalsLookup.get(context.reader().getCoreCacheKey());
    }

    @Override
    public BytesRef getValueByOrd(long ord) {
        throw new UnsupportedOperationException("can't do a ordinal to term lookup");
    }
}
