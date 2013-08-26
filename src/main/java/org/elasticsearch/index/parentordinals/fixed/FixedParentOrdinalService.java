package org.elasticsearch.index.parentordinals.fixed;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.CompoundTermsEnum;
import org.elasticsearch.index.parentordinals.ParentOrdinalService;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

/**
 */
public class FixedParentOrdinalService extends ParentOrdinalService {

    @Inject
    public FixedParentOrdinalService(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings, mapperService);
    }

    @Override
    public void doRefresh(IndicesWarmer.WarmerContext warmerContext, NavigableSet<HashedBytesArray> parentTypes) throws IOException {
        if (warmerContext.completeSearcher() == null) {
            return; // Invoked during a merge, we can bail b/c the normal warming will also invoke us.
        }

        Map<AtomicReader, Map<String, ParentOrdinals.Builder>> readerToTypesBuilder = new HashMap<AtomicReader, Map<String, ParentOrdinals.Builder>>();
        IndexReader indexReader = warmerContext.completeSearcher().reader();
        MultiTermsEnum termsEnum = CompoundTermsEnum.getCompoundTermsEnum(indexReader, UidFieldMapper.NAME, ParentFieldMapper.NAME);

        int ordinal = 0;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            HashedBytesArray[] typeAndId = Uid.splitUidIntoTypeAndId(term);
            if (!parentTypes.contains(typeAndId[0])) {
                do {
                    HashedBytesArray nextParent = parentTypes.ceiling(typeAndId[0]);
                    if (nextParent == null) {
                        break;
                    }

                    TermsEnum.SeekStatus status = termsEnum.seekCeil(nextParent.toBytesRef());
                    if (status == TermsEnum.SeekStatus.END) {
                        break;
                    } else if (status == TermsEnum.SeekStatus.NOT_FOUND) {
                        term = termsEnum.term();
                        typeAndId = Uid.splitUidIntoTypeAndId(term);
                    } else if (status == TermsEnum.SeekStatus.FOUND) {
                        assert false : "Seek status should never be FOUND, because we seek only the type part";
                        term = termsEnum.term();
                        typeAndId = Uid.splitUidIntoTypeAndId(term);
                    }
                } while (!parentTypes.contains(typeAndId[0]));
            }

            final int currentOrd = ++ordinal;
            MultiDocsEnum docsEnum = (MultiDocsEnum) termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                AtomicReaderContext context = indexReader.leaves().get(docsEnum.readerIndex());
                Map<String, ParentOrdinals.Builder> typeToOrdinalsBuilder = readerToTypesBuilder.get(context.reader());
                if (typeToOrdinalsBuilder == null) {
                    readerToTypesBuilder.put(context.reader(), typeToOrdinalsBuilder = new HashMap<String, ParentOrdinals.Builder>());
                }
                ParentOrdinals.Builder builder = typeToOrdinalsBuilder.get(typeAndId[0].toUtf8());
                if (builder == null) {
                    typeToOrdinalsBuilder.put(typeAndId[0].toUtf8(), builder = new ParentOrdinals.Builder(context.reader().maxDoc()));
                }

                builder.set(docsEnum.localDocID(), currentOrd);
            }
        }

        for (Map.Entry<AtomicReader, Map<String, ParentOrdinals.Builder>> entry : readerToTypesBuilder.entrySet()) {
            Map<String, ParentOrdinals> typeToParentOrdinals = new HashMap<String, ParentOrdinals>();
            for (Map.Entry<String, ParentOrdinals.Builder> entry1 : entry.getValue().entrySet()) {
                typeToParentOrdinals.put(entry1.getKey(), entry1.getValue().build());
            }
            AtomicReader reader = entry.getKey();
            if (reader instanceof SegmentReader) {
                ((SegmentReader) reader).addCoreClosedListener(this);
            }
            readerToTypes.put(reader.getCoreCacheKey(), typeToParentOrdinals);
        }
    }

    @Override
    public void onClose(Object owner) {
        readerToTypes.remove(owner);
    }
}
