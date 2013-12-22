package org.elasticsearch.index.parentordinals.fixed;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.index.parentordinals.TermsEnums;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

/**
 */
public class FixedParentOrdinals extends ParentOrdinals {

    public FixedParentOrdinals(Map<Object, Map<String, Segment>> readerToTypes) {
        super(readerToTypes);
    }

    public boolean supportsValueLookup() {
        return false;
    }

    public BytesRef parentValue(int ordinal, BytesRef spare) {
        return null;
    }

    public final static class Builder extends ParentOrdinals.Builder<FixedParentOrdinals> {

        @Inject
        public Builder(ShardId shardId, @IndexSettings Settings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public FixedParentOrdinals doBuild(FixedParentOrdinals current, IndicesWarmer.WarmerContext warmerContext, NavigableSet<BytesRef> parentTypes) throws IOException {
            if (warmerContext.completeSearcher() == null) {
                return current; // Invoked during a merge, we can bail b/c the normal warming will also invoke us.
            }

            Map<AtomicReader, Map<String, Segment.Builder>> readerToTypesBuilder = new HashMap<AtomicReader, Map<String, Segment.Builder>>();
            IndexReader indexReader = warmerContext.completeSearcher().reader();
            MultiTermsEnum termsEnum = TermsEnums.getCompoundTermsEnum(indexReader, parentTypes, UidFieldMapper.NAME, ParentFieldMapper.NAME);

            int ordinal = 0;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                String type = ((TermsEnums.ParentUidTermsEnum) ((MultiTermsEnum) termsEnum.getMatchArray()[0].terms).getMatchArray()[0].terms).type();
                final int currentOrd = ++ordinal;
                MultiDocsEnum docsEnum = (MultiDocsEnum) termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    AtomicReaderContext context = indexReader.leaves().get(docsEnum.readerIndex());
                    Map<String, Segment.Builder> typeToOrdinalsBuilder = readerToTypesBuilder.get(context.reader());
                    if (typeToOrdinalsBuilder == null) {
                        readerToTypesBuilder.put(context.reader(), typeToOrdinalsBuilder = new HashMap<String, Segment.Builder>());
                    }
                    Segment.Builder builder = typeToOrdinalsBuilder.get(type);
                    if (builder == null) {
                        typeToOrdinalsBuilder.put(type, builder = new Segment.Builder(context.reader().maxDoc()));
                    }

                    builder.set(docsEnum.localDocID(), currentOrd);
                }
            }

            Map<Object, Map<String, Segment>> readerToTypes = new HashMap<Object, Map<String, Segment>>(readerToTypesBuilder.size());
            for (Map.Entry<AtomicReader, Map<String, Segment.Builder>> entry : readerToTypesBuilder.entrySet()) {
                Map<String, Segment> typeToParentOrdinals = new HashMap<String, Segment>();
                for (Map.Entry<String, Segment.Builder> entry1 : entry.getValue().entrySet()) {
                    typeToParentOrdinals.put(entry1.getKey(), entry1.getValue().build());
                }
                AtomicReader reader = entry.getKey();
                readerToTypes.put(reader.getCoreCacheKey(), typeToParentOrdinals);
            }

            return new FixedParentOrdinals(readerToTypes);
        }
    }

}
