package org.elasticsearch.common.lucene.codec.bloom;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTHashMap;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class BloomFilteringPostingsFormat extends PostingsFormat {

    private final PostingsFormat delegatePostingsFormat;

    public BloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
        super("bloom1");
        this.delegatePostingsFormat = delegatePostingsFormat;
    }

    public BloomFilteringPostingsFormat() {
        super("bloom1");
        this.delegatePostingsFormat = null;
    }

    private static class WrappedTermsConsumer extends TermsConsumer {

        private TermsConsumer delegateTermsConsumer;
        private BloomFilter bloomFilter;

        public WrappedTermsConsumer(TermsConsumer termsConsumer, BloomFilter bloomFilter) {
            this.delegateTermsConsumer = termsConsumer;
            this.bloomFilter = bloomFilter;
        }

        public PostingsConsumer startTerm(BytesRef text) throws IOException {
            return delegateTermsConsumer.startTerm(text);
        }

        public void finishTerm(BytesRef text, TermStats stats) throws IOException {
            // Record this term in our BloomFilter
            if (stats.docFreq > 0) {
                bloomFilter.add(BytesRef.deepCopyOf(text));
            }
            delegateTermsConsumer.finishTerm(text, stats);
        }

        public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
            delegateTermsConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
        }

        public Comparator<BytesRef> getComparator() throws IOException {
            return delegateTermsConsumer.getComparator();
        }

    }

    private static class BloomFieldsConsumer extends FieldsConsumer {

        private final PostingsFormat delegatePostingsFormat;
        private final FieldsConsumer delegate;
        private final SegmentWriteState state;
        private final ExtTHashMap<FieldInfo, BloomFilter> bloomFilters;

        private BloomFieldsConsumer(PostingsFormat delegatePostingsFormat, SegmentWriteState state) throws IOException {
            this.delegatePostingsFormat = delegatePostingsFormat;
            this.delegate = delegatePostingsFormat.fieldsConsumer(state);
            this.state = state;
            this.bloomFilters = CacheRecycler.popHashMap();
        }

        @Override
        public TermsConsumer addField(FieldInfo field) throws IOException {
            BloomFilter bloomFilter = BloomFilterFactory.getFilter(state.segmentInfo.getDocCount(), 15);
            bloomFilters.put(field, bloomFilter);
            return new WrappedTermsConsumer(delegate.addField(field), bloomFilter);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            String bloomFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "blm1");
            IndexOutput bloomOutput = state.directory.createOutput(bloomFileName, state.context);
            CodecUtil.writeHeader(bloomOutput, "bloom1", 1);
            bloomOutput.writeString(delegatePostingsFormat.getName());
            bloomOutput.writeVInt(bloomFilters.size());
            for (Map.Entry<FieldInfo, BloomFilter> entries : bloomFilters.entrySet()) {
                bloomOutput.writeVInt(entries.getKey().number);
                entries.getValue().serialize(bloomOutput);
            }
            IOUtils.close(bloomOutput);
            CacheRecycler.pushHashMap(bloomFilters);
        }
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new BloomFieldsConsumer(delegatePostingsFormat, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new BloomFieldsProducer(state);
    }

    private static class BloomFilteredTerms extends Terms {

        private final Terms delegateTerms;
        private final BloomFilter filter;

        public BloomFilteredTerms(Terms terms, BloomFilter filter) {
            this.delegateTerms = terms;
            this.filter = filter;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
            return delegateTerms.intersect(compiled, startTerm);
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            if ((reuse != null) && (reuse instanceof BloomFilteredTermsEnum)) {
                // recycle the existing BloomFilteredTermsEnum by asking the delegate
                // to recycle its contained TermsEnum
                BloomFilteredTermsEnum bfte = (BloomFilteredTermsEnum) reuse;
                if (bfte.filter == filter) {
                    bfte.delegateTermsEnum = delegateTerms.iterator(bfte.delegateTermsEnum);
                    return bfte;
                }
            }
            // We have been handed something we cannot reuse (either null, wrong
            // class or wrong filter) so allocate a new object
            return new BloomFilteredTermsEnum(delegateTerms.iterator(reuse), filter);
        }

        @Override
        public Comparator<BytesRef> getComparator() throws IOException {
            return delegateTerms.getComparator();
        }

        @Override
        public long size() throws IOException {
            return delegateTerms.size();
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return delegateTerms.getSumTotalTermFreq();
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return delegateTerms.getSumDocFreq();
        }

        @Override
        public int getDocCount() throws IOException {
            return delegateTerms.getDocCount();
        }

        @Override
        public boolean hasOffsets() {
            return delegateTerms.hasOffsets();
        }

        @Override
        public boolean hasPositions() {
            return delegateTerms.hasPositions();
        }

        @Override
        public boolean hasPayloads() {
            return delegateTerms.hasPayloads();
        }

        private static class BloomFilteredTermsEnum extends TermsEnum {

            private TermsEnum delegateTermsEnum;
            private final BloomFilter filter;

            public BloomFilteredTermsEnum(TermsEnum iterator, BloomFilter filter) {
                this.delegateTermsEnum = iterator;
                this.filter = filter;
            }

            @Override
            public final BytesRef next() throws IOException {
                return delegateTermsEnum.next();
            }

            @Override
            public final Comparator<BytesRef> getComparator() {
                return delegateTermsEnum.getComparator();
            }

            @Override
            public final boolean seekExact(BytesRef text, boolean useCache) throws IOException {
                if (!filter.isPresent(text)) {
                    return false;
                } else {
                    return delegateTermsEnum.seekExact(text, useCache);
                }
            }

            @Override
            public final SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
                return delegateTermsEnum.seekCeil(text, useCache);
            }

            @Override
            public final void seekExact(long ord) throws IOException {
                delegateTermsEnum.seekExact(ord);
            }

            @Override
            public final BytesRef term() throws IOException {
                return delegateTermsEnum.term();
            }

            @Override
            public final long ord() throws IOException {
                return delegateTermsEnum.ord();
            }

            @Override
            public final int docFreq() throws IOException {
                return delegateTermsEnum.docFreq();
            }

            @Override
            public final long totalTermFreq() throws IOException {
                return delegateTermsEnum.totalTermFreq();
            }

            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
                return delegateTermsEnum.docsAndPositions(liveDocs, reuse, flags);
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
                return delegateTermsEnum.docs(liveDocs, reuse, flags);
            }

        }

    }

    private static class BloomFieldsProducer extends FieldsProducer {

        private final FieldsProducer delegate;
        private final ExtTHashMap<String, BloomFilter> bloomFilters;

        private BloomFieldsProducer(SegmentReadState state) throws IOException {
            this.bloomFilters = CacheRecycler.popHashMap();

            String bloomFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "blm1");
            IndexInput bloomIn = state.dir.openInput(bloomFileName, state.context);
            CodecUtil.checkHeader(bloomIn, "bloom1", 1, 1);
            PostingsFormat delegatePostingsFormat = PostingsFormat.forName(bloomIn.readString());
            delegate = delegatePostingsFormat.fieldsProducer(state);
            int numBlooms = bloomIn.readVInt();
            for (int i = 0; i < numBlooms; i++) {
                int fieldNum = bloomIn.readVInt();
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                BloomFilter bloomFilters = ObsBloomFilter.deSerialize(bloomIn);
                this.bloomFilters.put(fieldInfo.name, bloomFilters);
            }
            IOUtils.close(bloomIn);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            CacheRecycler.pushHashMap(bloomFilters);
        }

        @Override
        public Iterator<String> iterator() {
            return delegate.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            BloomFilter bloomFilter = bloomFilters.get(field);
            if (bloomFilter == null) {
                return delegate.terms(field);
            } else {
                Terms result = delegate.terms(field);
                if (result == null) {
                    return null;
                }
                return new BloomFilteredTerms(result, bloomFilter);
            }
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public long getUniqueTermCount() throws IOException {
            return delegate.getUniqueTermCount();
        }
    }

}
