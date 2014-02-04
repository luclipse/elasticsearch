package org.elasticsearch.index.fielddata.global;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.bucket.BytesRefHash;

/**
 */
public class GlobalNominalsAtomicFieldData implements GlobalReference {

    public static class Builder {

        private final ImmutableOpenMap.Builder<Object, Ordinals> segmentToOrdinals;

        public Builder() {
            segmentToOrdinals = ImmutableOpenMap.builder();
        }

        public Builder(GlobalNominalsAtomicFieldData previous) {
            this.segmentToOrdinals = ImmutableOpenMap.builder(previous.segmentToOrdinals);
        }

        public boolean skipSegement(AtomicReaderContext leaf) {
            return segmentToOrdinals.containsKey(leaf.reader().getCoreCacheKey());
        }

        public void addSegment(AtomicReaderContext context, Ordinals ordinals) {
            segmentToOrdinals.put(context.reader().getCoreCacheKey(), ordinals);
        }

        public GlobalNominalsAtomicFieldData build(BytesRefHash terms) {
            return new GlobalNominalsAtomicFieldData(terms, segmentToOrdinals.build());
        }

    }

    private final BytesRef spare;
    private final BytesRefHash terms;
    private final ImmutableOpenMap<Object, Ordinals> segmentToOrdinals;

    public GlobalNominalsAtomicFieldData(BytesRefHash terms, ImmutableOpenMap<Object, Ordinals> segmentToOrdinals) {
        this.spare = new BytesRef();
        this.terms = terms;
        this.segmentToOrdinals = segmentToOrdinals;
    }

    @Override
    public Ordinals ordinals(AtomicReaderContext context) {
        return segmentToOrdinals.get(context.reader().getCoreCacheKey());
    }

    @Override
    public BytesRef getValueByOrd(long ord) {
        return terms.get(ord, spare);
    }
}
