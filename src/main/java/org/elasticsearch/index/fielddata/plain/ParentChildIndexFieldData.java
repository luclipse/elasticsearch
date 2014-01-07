package org.elasticsearch.index.fielddata.plain;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AbstractIndexFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 */
public class ParentChildIndexFieldData extends AbstractIndexFieldData<ParentChildAtomicFieldData> implements DocumentTypeListener, IndexFieldData.WithOrdinals<ParentChildAtomicFieldData> {

    private final NavigableSet<BytesRef> parentTypes;
    private final CircuitBreakerService breakerService;

    // If child type (a type with _parent field) is added or removed, we want to make sure modifications don't happen
    // while loading.
    private final Object lock = new Object();

    public ParentChildIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache, MapperService mapperService, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        parentTypes = new TreeSet<BytesRef>(BytesRef.getUTF8SortedAsUnicodeComparator());
        this.breakerService = breakerService;
        for (DocumentMapper documentMapper : mapperService) {
            beforeCreate(documentMapper);
        }
        mapperService.addTypeListener(this);
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        throw new ElasticsearchIllegalArgumentException("can't sort on _parent field");
    }

    @Override
    public ParentChildAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();
        // TODO: Implement a custom estimator for p/c field data
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker());
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat(
                "acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO
        );

        synchronized (lock) {
            boolean success = false;
            ParentChildAtomicFieldData data = null;
            MultiTermsEnum termsEnum = getCompoundTermsEnum(reader, parentTypes, UidFieldMapper.NAME, ParentFieldMapper.NAME);
            ObjectObjectOpenHashMap<String, TypeBuilder> typeBuilders = ObjectObjectOpenHashMap.newInstance();
            try {
                DocsEnum docsEnum = null;
                for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                    String type = ((ParentUidTermsEnum) termsEnum.getMatchArray()[0].terms).type();
                    TypeBuilder typeBuilder = typeBuilders.get(type);
                    if (typeBuilder == null) {
                        typeBuilders.put(type, typeBuilder = new TypeBuilder(acceptableTransientOverheadRatio, reader));
                    }

                    BytesRef id = ((ParentUidTermsEnum) termsEnum.getMatchArray()[0].terms).id();
                    final long termOrd = typeBuilder.builder.nextOrdinal();
                    assert termOrd == typeBuilder.termOrdToBytesOffset.size();
                    typeBuilder.termOrdToBytesOffset.add(typeBuilder.bytes.copyUsingLengthPrefix(id));
                    docsEnum = termsEnum.docs(null, docsEnum, DocsEnum.FLAG_NONE);
                    for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                        typeBuilder.builder.addDoc(docId);
                    }
                }

                ImmutableOpenMap.Builder<String, PagedBytesAtomicFieldData> typeToAtomicFieldData = ImmutableOpenMap.builder(typeBuilders.size());
                for (ObjectObjectCursor<String, TypeBuilder> cursor : typeBuilders) {
                    final long sizePointer = cursor.value.bytes.getPointer();
                    PagedBytes.Reader bytesReader = cursor.value.bytes.freeze(true);
                    final Ordinals ordinals = cursor.value.builder.build(fieldDataType.getSettings());

                    typeToAtomicFieldData.put(
                            cursor.key,
                            new PagedBytesAtomicFieldData(bytesReader, sizePointer, cursor.value.termOrdToBytesOffset, ordinals)
                    );
                }
                data = new ParentChildAtomicFieldData(typeToAtomicFieldData.build());
                success = true;
                return data;
            } finally {
                for (ObjectObjectCursor<String, TypeBuilder> cursor : typeBuilders) {
                    cursor.value.builder.close();
                    if (success) {
                        estimator.afterLoad(null, data.getMemorySizeInBytes());
                    }
                }
            }
        }
    }

    @Override
    public void beforeCreate(DocumentMapper mapper) {
        synchronized (lock) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                // A _parent field can never be added to an existing mapping, so a _parent field either exists on
                // a new created or doesn't exists. This is why we can update the known parent types via DocumentTypeListener
                if (parentTypes.add(new BytesRef(parentFieldMapper.type()))) {
                    clear();
                }
            }
        }
    }

    @Override
    public void afterRemove(DocumentMapper mapper) {
        synchronized (lock) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentTypes.remove(new BytesRef(parentFieldMapper.type()));
            }
        }
    }

    public static MultiTermsEnum getCompoundTermsEnum(AtomicReader atomicReader, NavigableSet<BytesRef> parentTypes, String... fields) throws IOException {
        List<TermsEnum> fieldEnums = new ArrayList<TermsEnum>();
        for (int i = 0; i < fields.length; i++) {
            Terms terms = atomicReader.terms(fields[i]);
            if (terms != null) {
                fieldEnums.add(new ParentUidTermsEnum(terms.iterator(null), parentTypes));
            }
        }

        ReaderSlice[] slices = new ReaderSlice[fieldEnums.size()];
        MultiTermsEnum.TermsEnumIndex[] indexes = new MultiTermsEnum.TermsEnumIndex[fieldEnums.size()];
        for (int j = 0; j < slices.length; j++) {
            slices[j] = new ReaderSlice(0, 0, j);
            indexes[j] = new MultiTermsEnum.TermsEnumIndex(fieldEnums.get(j), j);
        }

        MultiTermsEnum termsEnum = new MultiTermsEnum(slices);
        termsEnum.reset(indexes);
        return termsEnum;
    }

    class TypeBuilder {

        final PagedBytes bytes;
        final MonotonicAppendingLongBuffer termOrdToBytesOffset;
        final OrdinalsBuilder builder;

        TypeBuilder(float acceptableTransientOverheadRatio, AtomicReader reader) throws IOException {
            bytes = new PagedBytes(15);
            termOrdToBytesOffset = new MonotonicAppendingLongBuffer();
            termOrdToBytesOffset.add(0); // first ord is reserved for missing values
            // 0 is reserved for "unset"
            bytes.copyUsingLengthPrefix(new BytesRef());
            builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio);
        }
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new ParentChildIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, mapperService, breakerService);
        }
    }

    public static final class ParentUidTermsEnum extends FilteredTermsEnum {

        private final NavigableSet<BytesRef> parentTypes;
        private BytesRef seekTerm;
        private String type;
        private BytesRef id;

        ParentUidTermsEnum(TermsEnum tenum, NavigableSet<BytesRef> parentTypes) {
            super(tenum, true);
            this.parentTypes = parentTypes;
            this.seekTerm = parentTypes.isEmpty() ? null : parentTypes.first();
        }

        @Override
        protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
            BytesRef temp = seekTerm;
            seekTerm = null;
            return temp;
        }

        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
            BytesRef[] typeAndId = Uid.splitUidIntoTypeAndId_br(term);
            if (parentTypes.contains(typeAndId[0])) {
                type = typeAndId[0].utf8ToString();
                id = typeAndId[1];
                return AcceptStatus.YES;
            } else {
                BytesRef nextType = parentTypes.ceiling(typeAndId[0]);
                if (nextType == null) {
                    return AcceptStatus.END;
                }
                seekTerm = nextType;
                return AcceptStatus.NO_AND_SEEK;
            }
        }

        public String type() {
            return type;
        }

        public BytesRef id() {
            return id;
        }

    }

}
