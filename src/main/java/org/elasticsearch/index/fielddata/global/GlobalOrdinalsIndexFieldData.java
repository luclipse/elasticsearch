package org.elasticsearch.index.fielddata.global;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

/**
 */
public class GlobalOrdinalsIndexFieldData extends TopLevelIndexFieldData<GlobalOrdinalsReference> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public BaseFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                    IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new GlobalOrdinalsIndexFieldData(index, mapper.fieldDataType(), mapper.names(), breakerService);
        }
    }

    public GlobalOrdinalsIndexFieldData(Index index, FieldDataType fieldDataType, FieldMapper.Names fieldNames, CircuitBreakerService breakerService) {
        super(index, fieldNames, fieldDataType, breakerService);
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return null;
    }

    @Override
    public GlobalOrdinalsReference loadDirect(IndexReader indexReader, GlobalOrdinalsReference previous) throws Exception {
        MultiTermsEnum termsEnum = TermsEnums.getCompoundTermsEnum(indexReader, getFieldNames().indexName());

        long ordinalCounter = 0;
        ObjectObjectOpenHashMap<Object, OrdinalsBuilder> segmentToOrdinalsBuilder = ObjectObjectOpenHashMap.newInstance();
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            final long currentOrd = ++ordinalCounter;
            MultiDocsEnum docsEnum = (MultiDocsEnum) termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                AtomicReaderContext subContext = indexReader.leaves().get(docsEnum.currentReaderSlice());
                AtomicReader subReader = subContext.reader();

                OrdinalsBuilder ordinalsBuilder = segmentToOrdinalsBuilder.get(subReader.getCoreCacheKey());
                if (ordinalsBuilder == null) {
                    segmentToOrdinalsBuilder.put(subReader.getCoreCacheKey(), ordinalsBuilder = new OrdinalsBuilder(subReader.maxDoc()));
                }

                ordinalsBuilder.addDoc(docsEnum.docID() - docsEnum.currentBase(), currentOrd);
            }
        }

        ImmutableOpenMap.Builder<Object, Ordinals> builder = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<Object, OrdinalsBuilder> cursor : segmentToOrdinalsBuilder) {
            builder.put(cursor.key, cursor.value.build(fieldDataType.getSettings()));
        }
        return new GlobalOrdinalsReference(builder.build());
    }
}
