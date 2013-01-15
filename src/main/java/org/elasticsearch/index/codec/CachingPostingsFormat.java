package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * A postings formats that only caches the TermsEnum in a thread local cache.
 * All read and write operations are performed by the delete postings format.
 */
public class CachingPostingsFormat extends PostingsFormat {

    private final PostingsFormat delegate;

    public CachingPostingsFormat(PostingsFormat delegate) {
        super("caching");
        this.delegate = delegate;
    }

    public CachingPostingsFormat() {
        super("caching");
        this.delegate = null;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new CachingFieldsConsumer(state, delegate);
    }

    static class CachingFieldsConsumer extends FieldsConsumer {

        private final SegmentWriteState state;
        private final PostingsFormat delegatePostingsFormat;
        private final FieldsConsumer delegateFieldsConsumer;

        CachingFieldsConsumer(SegmentWriteState state, PostingsFormat delegate) throws IOException {
            this.state = state;
            this.delegatePostingsFormat = delegate;
            this.delegateFieldsConsumer = delegate.fieldsConsumer(state);
        }

        @Override
        public TermsConsumer addField(FieldInfo field) throws IOException {
            return delegateFieldsConsumer.addField(field);
        }

        @Override
        public void close() throws IOException {
            delegateFieldsConsumer.close();
            String delegateFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, "martijn"
            );
            IndexOutput indexOutput = state.directory.createOutput(delegateFileName, state.context);
            CodecUtil.writeHeader(indexOutput, "caching", 1);
            indexOutput.writeString(delegatePostingsFormat.getName());
            IOUtils.close(indexOutput);
        }
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new CachingFieldsProducer(state);
    }

    static class CachingFieldsProducer extends FieldsProducer {

        // We can keep this here, reference is kept in SegmentCoreReaders as a field
        // Not static, b/c we have now one per segment / field combination.
        private final CloseableThreadLocal<TermsEnum> cachedTermsEnums = new CloseableThreadLocal<TermsEnum>();

        private final SegmentReadState state;
        private final FieldsProducer delegate;

        CachingFieldsProducer(SegmentReadState state) throws IOException {
            this.state = state;
            String delegateFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, "martijn"
            );
            IndexInput indexInput = state.dir.openInput(delegateFileName, state.context);
            CodecUtil.checkHeader(indexInput, "caching", 1, 1);
            PostingsFormat delegatePostingsFormat = PostingsFormat.forName(indexInput.readString());
            delegate = delegatePostingsFormat.fieldsProducer(state);
            IOUtils.close(indexInput);
        }

        @Override
        public void close() throws IOException {
            cachedTermsEnums.close();
            delegate.close();
        }

        @Override
        public Iterator<String> iterator() {
            return delegate.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return new CachingTerms(delegate.terms(field), cachedTermsEnums);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public long getUniqueTermCount() throws IOException {
            return delegate.getUniqueTermCount();
        }

        static class CachingTerms extends Terms {

            private final Terms delegate;
            private final CloseableThreadLocal<TermsEnum> cachedTermsEnums;

            public CachingTerms(Terms delegate, CloseableThreadLocal<TermsEnum> cachedTermsEnums) {
                this.delegate = delegate;
                this.cachedTermsEnums = cachedTermsEnums;
            }

            @Override
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
                TermsEnum termsEnum = cachedTermsEnums.get();
                if (termsEnum != null) {
                    return termsEnum;
                }
                termsEnum = delegate.iterator(reuse);
                cachedTermsEnums.set(termsEnum);
                return new CachingTermsEnum(this, termsEnum, reuse);
            }

            @Override
            public Comparator<BytesRef> getComparator() throws IOException {
                return delegate.getComparator();
            }

            @Override
            public long size() throws IOException {
                return delegate.size();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                return delegate.getSumTotalTermFreq();
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return delegate.getSumDocFreq();
            }

            @Override
            public int getDocCount() throws IOException {
                return delegate.getDocCount();
            }

            @Override
            public boolean hasOffsets() {
                return delegate.hasOffsets();
            }

            @Override
            public boolean hasPositions() {
                return delegate.hasPositions();
            }

            @Override
            public boolean hasPayloads() {
                return delegate.hasPayloads();
            }

            static class CachingTermsEnum extends TermsEnum {

                private final Terms delegateTerms;
                private final TermsEnum cachedTermsEnum;
                private final TermsEnum reuse;

                private TermsEnum termsEnum;

                CachingTermsEnum(Terms delegateTerms, TermsEnum cachedTermsEnum, TermsEnum reuse) {
                    this.delegateTerms = delegateTerms;
                    this.cachedTermsEnum = cachedTermsEnum;
                    this.reuse = reuse;
                }

                @Override
                public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.seekExact(text, useCache);
                }

                @Override
                public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.seekCeil(text, useCache);
                }

                @Override
                public void seekExact(long ord) throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    actual.seekExact(ord);
                }

                @Override
                public BytesRef term() throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.term();
                }

                @Override
                public long ord() throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.ord();
                }

                @Override
                public int docFreq() throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.docFreq();
                }

                @Override
                public long totalTermFreq() throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.totalTermFreq();
                }

                @Override
                public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.docs(liveDocs, reuse, flags);
                }

                @Override
                public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.docsAndPositions(liveDocs, reuse, flags);
                }

                @Override
                public BytesRef next() throws IOException {
                    if (termsEnum == null) {
                        termsEnum = delegateTerms.iterator(reuse);
                    }
                    return termsEnum.next();
                }

                @Override
                public Comparator<BytesRef> getComparator() {
                    TermsEnum actual = termsEnum != null ? termsEnum : cachedTermsEnum;
                    return actual.getComparator();
                }
            }

        }

    }
}
