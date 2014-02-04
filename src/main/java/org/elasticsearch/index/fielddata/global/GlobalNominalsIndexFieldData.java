package org.elasticsearch.index.fielddata.global;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.bucket.BytesRefHash;

import java.util.List;

/**
 */
public class GlobalNominalsIndexFieldData extends TopLevelIndexFieldData<GlobalNominalsAtomicFieldData> implements SegmentReader.CoreClosedListener {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public BaseFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                    IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            double optimizeParentValuesRatio = indexSettings.getAsDouble("index.rebuild_parent_values_ratio", 0.5);
            return new GlobalNominalsIndexFieldData(index, mapper.names(), mapper.fieldDataType(), breakerService, optimizeParentValuesRatio);
        }

    }

    private final double optimizeParentValuesRatio;

    BytesRefHash terms;

    public GlobalNominalsIndexFieldData(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, CircuitBreakerService breakerService, double optimizeParentValuesRatio) {
        super(index, fieldNames, fieldDataType, breakerService);
        this.optimizeParentValuesRatio = optimizeParentValuesRatio;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        throw new UnsupportedOperationException("Global nominals don't support sorting");
    }

    @Override
    public GlobalNominalsAtomicFieldData loadDirect(IndexReader indexReader, GlobalNominalsAtomicFieldData previous) throws Exception {
        GlobalNominalsAtomicFieldData.Builder builder;
        if (previous == null) {
            terms = new BytesRefHash(50, null);
            builder = new GlobalNominalsAtomicFieldData.Builder();
        } else {
            builder = new GlobalNominalsAtomicFieldData.Builder(previous);
        }

        List<AtomicReaderContext> leaves = indexReader.leaves();
        for (AtomicReaderContext leaf : leaves) {
            if (builder.skipSegement(leaf)) {
                continue;
            }

            AtomicReader reader = leaf.reader();
            Terms terms = reader.terms(getFieldNames().indexName());
            if (terms == null) {
                continue;
            }

            OrdinalsBuilder ordinalsBuilder = new OrdinalsBuilder(reader.maxDoc());
            TermsEnum termsEnum = terms.iterator(null);
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                long currentOrd = this.terms.add(term);
                if (currentOrd < 0) {
                    currentOrd = ;
                }
                ordinalsBuilder.setOrdinal(currentOrd);
                DocsEnum docsEnum = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    ordinalsBuilder.addDoc(docId);
                }
            }
            SegmentReaderUtils.registerCoreListener(reader, this);
        }

        return builder.build(terms);
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) {

    }

}