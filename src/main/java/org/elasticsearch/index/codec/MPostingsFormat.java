package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.bloom.BloomFilterFactory;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.*;

public final class MPostingsFormat extends PostingsFormat {

    public static final String BLOOM_CODEC_NAME = "BloomFilter1";
    public static final int BLOOM_CODEC_VERSION = 1;

    /**
     * Extension of Bloom Filters file
     */
    static final String BLOOM_EXTENSION = "blm";

    BloomFilterFactory bloomFilterFactory = new DefaultBloomFilterFactory();
    private PostingsFormat delegatePostingsFormat;

    /**
     * Creates Bloom filters for a selection of fields created in the index. This
     * is recorded as a set of Bitsets held as a segment summary in an additional
     * "blm" file. This PostingsFormat delegates to a choice of delegate
     * PostingsFormat for encoding all other postings data.
     *
     * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data i.e.
     *                               postings info.
     * @param bloomFilterFactory     The {@link BloomFilterFactory} responsible for sizing BloomFilters
     *                               appropriately
     */
    public MPostingsFormat(PostingsFormat delegatePostingsFormat,
                           BloomFilterFactory bloomFilterFactory) {
        super(BLOOM_CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.bloomFilterFactory = bloomFilterFactory;
    }

    /**
     * Creates Bloom filters for a selection of fields created in the index. This
     * is recorded as a set of Bitsets held as a segment summary in an additional
     * "blm" file. This PostingsFormat delegates to a choice of delegate
     * PostingsFormat for encoding all other postings data. This choice of
     * constructor defaults to the {@link DefaultBloomFilterFactory} for
     * configuring per-field BloomFilters.
     *
     * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data i.e.
     *                               postings info.
     */
    public MPostingsFormat(PostingsFormat delegatePostingsFormat) {
        this(delegatePostingsFormat, new DefaultBloomFilterFactory());
    }

    // Used only by core Lucene at read-time via Service Provider instantiation -
    // do not use at Write-time in application code.
    public MPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    public FieldsConsumer fieldsConsumer(SegmentWriteState state)
            throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        return new BloomFilteredFieldsConsumer(
                delegatePostingsFormat.fieldsConsumer(state), state,
                delegatePostingsFormat);
    }

    public FieldsProducer fieldsProducer(SegmentReadState state)
            throws IOException {
        return new BloomFilteredFieldsProducer(state);
    }

    static public class BloomFilteredFieldsProducer extends FieldsProducer {

        private FieldsProducer delegateFieldsProducer;
        final HashMap<String, FuzzySet> bloomsByFieldName = new HashMap<String, FuzzySet>();

        public BloomFilteredFieldsProducer(SegmentReadState state)
                throws IOException {

            String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            IndexInput bloomIn = null;
            boolean success = false;
            try {
                bloomIn = state.dir.openInput(bloomFileName, state.context);
                CodecUtil.checkHeader(bloomIn, BLOOM_CODEC_NAME, BLOOM_CODEC_VERSION,
                        BLOOM_CODEC_VERSION);
                // // Load the hash function used in the BloomFilter
                // hashFunction = HashFunction.forName(bloomIn.readString());
                // Load the delegate postings format
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(bloomIn
                        .readString());

                this.delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);
                int numBlooms = bloomIn.readInt();
                for (int i = 0; i < numBlooms; i++) {
                    int fieldNum = bloomIn.readInt();
                    FuzzySet bloom = FuzzySet.deserialize(bloomIn);
                    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                    bloomsByFieldName.put(fieldInfo.name, bloom);
                }
                IOUtils.close(bloomIn);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(bloomIn, delegateFieldsProducer);
                }
            }
        }

        public Iterator<String> iterator() {
            return delegateFieldsProducer.iterator();
        }

        public void close() throws IOException {
            delegateFieldsProducer.close();
        }

        public Terms terms(String field) throws IOException {
            FuzzySet filter = bloomsByFieldName.get(field);
            if (filter == null) {
                return delegateFieldsProducer.terms(field);
            } else {
                Terms result = delegateFieldsProducer.terms(field);
                if (result == null) {
                    return null;
                }
                return new BloomFilteredTerms(result, filter);
            }
        }

        public int size() {
            return delegateFieldsProducer.size();
        }

        public long getUniqueTermCount() throws IOException {
            return delegateFieldsProducer.getUniqueTermCount();
        }

        static class BloomFilteredTerms extends Terms {

            private final Terms delegateTerms;
            private final FuzzySet filter;

            public BloomFilteredTerms(Terms terms, FuzzySet filter) {
                this.delegateTerms = terms;
                this.filter = filter;
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled,
                                       final BytesRef startTerm) throws IOException {
                return delegateTerms.intersect(compiled, startTerm);
            }

            @Override
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
                TermsEnum result;
                if ((reuse != null) && (reuse instanceof LazyTermsEnum)) {
                    // recycle the existing BloomFilteredTermsEnum by asking the delegate
                    // to recycle its contained TermsEnum
                    LazyTermsEnum bfte = (LazyTermsEnum) reuse;
                    if (bfte.filter == filter) {
                        bfte.delegateTermsEnum = delegateTerms
                                .iterator(bfte.delegateTermsEnum);
                        return bfte;
                    }
                }
                // We have been handed something we cannot reuse (either null, wrong
                // class or wrong filter) so allocate a new object
//                result = new LazyTermsEnum(delegateTerms, filter);
                result = new DelegateTermsEnum(delegateTerms.iterator(reuse), filter);
                return result;
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
        }

        static class DelegateTermsEnum extends TermsEnum {

            private final FuzzySet filter;
            private final TermsEnum delegateTermsEnum;

            public DelegateTermsEnum(TermsEnum delegateTermEnum, FuzzySet filter) {
                this.delegateTermsEnum = delegateTermEnum;
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
            public final boolean seekExact(BytesRef text, boolean useCache)
                    throws IOException {
                // The magical fail-fast speed up that is the entire point of all of
                // this code - save a disk seek if there is a match on an in-memory
                // structure
                // that may occasionally give a false positive but guaranteed no false
                // negatives
                if (filter.contains(text) == FuzzySet.ContainsResult.NO) {
                    return false;
                }
                return delegateTermsEnum.seekExact(text, useCache);
            }

            @Override
            public final SeekStatus seekCeil(BytesRef text, boolean useCache)
                    throws IOException {
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
            public TermState termState() throws IOException {
                return delegateTermsEnum.termState();
            }

            @Override
            public void seekExact(BytesRef term, TermState state) throws IOException {
                delegateTermsEnum.seekExact(term, state);
            }

            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
                                                         DocsAndPositionsEnum reuse, int flags) throws IOException {
                return delegateTermsEnum.docsAndPositions(liveDocs, reuse, flags);
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
                    throws IOException {
                return delegateTermsEnum.docs(liveDocs, reuse, flags);
            }


        }

        static class LazyTermsEnum extends TermsEnum {

            private final FuzzySet filter;
            private final Terms delegateTerm;
            TermsEnum delegateTermsEnum;

            public LazyTermsEnum(Terms delegateTerm, FuzzySet filter) {
                this.delegateTerm = delegateTerm;
                this.filter = filter;
            }

            @Override
            public final BytesRef next() throws IOException {
                if (delegateTermsEnum != null) {
                    return delegateTermsEnum.next();
                } else {
                    delegateTermsEnum = delegateTerm.iterator(null);
                    return delegateTermsEnum.next();
                }
            }

            @Override
            public final Comparator<BytesRef> getComparator() {
                try {
                    if (delegateTermsEnum != null) {
                        return delegateTermsEnum.getComparator();
                    } else {
                        delegateTermsEnum = delegateTerm.iterator(null);
                        return delegateTermsEnum.getComparator();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public final boolean seekExact(BytesRef text, boolean useCache)
                    throws IOException {
                // The magical fail-fast speed up that is the entire point of all of
                // this code - save a disk seek if there is a match on an in-memory
                // structure
                // that may occasionally give a false positive but guaranteed no false
                // negatives
                if (filter.contains(text) == FuzzySet.ContainsResult.NO) {
                    return false;
                }
                if (delegateTermsEnum != null) {
                    return delegateTermsEnum.seekExact(text, useCache);
                } else {
                    delegateTermsEnum = delegateTerm.iterator(null);
                    return delegateTermsEnum.seekExact(text, useCache);
                }
            }

            @Override
            public final SeekStatus seekCeil(BytesRef text, boolean useCache)
                    throws IOException {
                if (delegateTermsEnum != null) {
                    return delegateTermsEnum.seekCeil(text, useCache);
                } else {
                    delegateTermsEnum = delegateTerm.iterator(null);
                    return delegateTermsEnum.seekCeil(text, useCache);
                }
            }

            @Override
            public final void seekExact(long ord) throws IOException {
                if (delegateTermsEnum != null) {
                    delegateTermsEnum.seekExact(ord);
                } else {
                    delegateTermsEnum = delegateTerm.iterator(null);
                    delegateTermsEnum.seekExact(ord);
                }
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
            public TermState termState() throws IOException {
                return delegateTermsEnum.termState();
            }

            @Override
            public void seekExact(BytesRef term, TermState state) throws IOException {
                delegateTermsEnum.seekExact(term, state);
            }

            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
                                                         DocsAndPositionsEnum reuse, int flags) throws IOException {
                return delegateTermsEnum.docsAndPositions(liveDocs, reuse, flags);
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
                    throws IOException {
                return delegateTermsEnum.docs(liveDocs, reuse, flags);
            }


        }

    }

    class BloomFilteredFieldsConsumer extends FieldsConsumer {

        private final FieldsConsumer delegateFieldsConsumer;
        private final Map<FieldInfo, FuzzySet> bloomFilters = new HashMap<FieldInfo, FuzzySet>();
        private final SegmentWriteState state;

        // private PostingsFormat delegatePostingsFormat;

        public BloomFilteredFieldsConsumer(FieldsConsumer fieldsConsumer,
                                           SegmentWriteState state, PostingsFormat delegatePostingsFormat) {
            this.delegateFieldsConsumer = fieldsConsumer;
            // this.delegatePostingsFormat=delegatePostingsFormat;
            this.state = state;
        }

        @Override
        public TermsConsumer addField(FieldInfo field) throws IOException {
            FuzzySet bloomFilter = bloomFilterFactory.getSetForField(state, field);
            if (bloomFilter != null) {
                assert bloomFilters.containsKey(field) == false;
                bloomFilters.put(field, bloomFilter);
                return new WrappedTermsConsumer(delegateFieldsConsumer.addField(field), bloomFilter);
            } else {
                // No, use the unfiltered fieldsConsumer - we are not interested in
                // recording any term Bitsets.
                return delegateFieldsConsumer.addField(field);
            }
        }

        @Override
        public void close() throws IOException {
            delegateFieldsConsumer.close();
            // Now we are done accumulating values for these fields
            List<Map.Entry<FieldInfo, FuzzySet>> nonSaturatedBlooms = new ArrayList<Map.Entry<FieldInfo, FuzzySet>>();

            for (Map.Entry<FieldInfo, FuzzySet> entry : bloomFilters.entrySet()) {
                FuzzySet bloomFilter = entry.getValue();
                if (!bloomFilterFactory.isSaturated(bloomFilter, entry.getKey())) {
                    nonSaturatedBlooms.add(entry);
                }
            }
            String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            IndexOutput bloomOutput = null;
            try {
                bloomOutput = state.directory
                        .createOutput(bloomFileName, state.context);
                CodecUtil.writeHeader(bloomOutput, BLOOM_CODEC_NAME,
                        BLOOM_CODEC_VERSION);
                // remember the name of the postings format we will delegate to
                bloomOutput.writeString(delegatePostingsFormat.getName());

                // First field in the output file is the number of fields+blooms saved
                bloomOutput.writeInt(nonSaturatedBlooms.size());
                for (Map.Entry<FieldInfo, FuzzySet> entry : nonSaturatedBlooms) {
                    FieldInfo fieldInfo = entry.getKey();
                    FuzzySet bloomFilter = entry.getValue();
                    bloomOutput.writeInt(fieldInfo.number);
                    saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
                }
            } finally {
                IOUtils.close(bloomOutput);
            }
            //We are done with large bitsets so no need to keep them hanging around
            bloomFilters.clear();
        }

        private void saveAppropriatelySizedBloomFilter(IndexOutput bloomOutput,
                                                       FuzzySet bloomFilter, FieldInfo fieldInfo) throws IOException {

            FuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo,
                    bloomFilter);
            if (rightSizedSet == null) {
                rightSizedSet = bloomFilter;
            }
            rightSizedSet.serialize(bloomOutput);
        }

    }

    class WrappedTermsConsumer extends TermsConsumer {

        private final TermsConsumer delegateTermsConsumer;
        private final FuzzySet bloomFilter;

        public WrappedTermsConsumer(TermsConsumer termsConsumer, FuzzySet bloomFilter) {
            this.delegateTermsConsumer = termsConsumer;
            this.bloomFilter = bloomFilter;
        }

        public PostingsConsumer startTerm(BytesRef text) throws IOException {
            return delegateTermsConsumer.startTerm(text);
        }

        public void finishTerm(BytesRef text, TermStats stats) throws IOException {

            // Record this term in our BloomFilter
            if (stats.docFreq > 0) {
                bloomFilter.addValue(text);
            }
            delegateTermsConsumer.finishTerm(text, stats);
        }

        public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
                throws IOException {
            delegateTermsConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
        }

        public Comparator<BytesRef> getComparator() throws IOException {
            return delegateTermsConsumer.getComparator();
        }

    }

}
