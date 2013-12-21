package org.elasticsearch.index.parentordinals.fixed;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.TermsEnums;
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
    public void doRefresh(IndicesWarmer.WarmerContext warmerContext, NavigableSet<BytesRef> parentTypes) throws IOException {
        if (warmerContext.completeSearcher() == null) {
            return; // Invoked during a merge, we can bail b/c the normal warming will also invoke us.
        }

        Map<AtomicReader, Map<String, ParentOrdinals.Builder>> readerToTypesBuilder = new HashMap<AtomicReader, Map<String, ParentOrdinals.Builder>>();
        IndexReader indexReader = warmerContext.completeSearcher().reader();
        MultiTermsEnum termsEnum = TermsEnums.getCompoundTermsEnum(indexReader, parentTypes, UidFieldMapper.NAME, ParentFieldMapper.NAME);

        int ordinal = 0;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            String type = ((TermsEnums.ParentUidTermsEnum) ((MultiTermsEnum) termsEnum.getMatchArray()[0].terms).getMatchArray()[0].terms).type();
            final int currentOrd = ++ordinal;
            MultiDocsEnum docsEnum = (MultiDocsEnum) termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
            for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                AtomicReaderContext context = indexReader.leaves().get(docsEnum.readerIndex());
                Map<String, ParentOrdinals.Builder> typeToOrdinalsBuilder = readerToTypesBuilder.get(context.reader());
                if (typeToOrdinalsBuilder == null) {
                    readerToTypesBuilder.put(context.reader(), typeToOrdinalsBuilder = new HashMap<String, ParentOrdinals.Builder>());
                }
                ParentOrdinals.Builder builder = typeToOrdinalsBuilder.get(type);
                if (builder == null) {
                    typeToOrdinalsBuilder.put(type, builder = new ParentOrdinals.Builder(context.reader().maxDoc()));
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
