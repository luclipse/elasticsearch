package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A postings formats that only caches the TermsEnum in a thread local cache.
 * All read and write operations are performed by the delete postings format.
 */
public class CachingPostingsFormat extends PostingsFormat {

    private static ThreadLocal<Map<Object, TermsEnum>> cachedTermsEnums = new ThreadLocal<Map<Object, TermsEnum>>() {

        @Override
        protected Map<Object, TermsEnum> initialValue() {
            return new HashMap<Object, TermsEnum>();
        }

    };

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
                    state.segmentInfo.name, state.segmentSuffix, "fantastic"
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

        private final SegmentReadState state;
        private final FieldsProducer delegate;

        CachingFieldsProducer(SegmentReadState state) throws IOException {
            this.state = state;
            String delegateFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, "fantastic"
            );
            IndexInput indexInput = state.dir.openInput(delegateFileName, state.context);
            CodecUtil.checkHeader(indexInput, "caching", 1, 1);
            PostingsFormat delegatePostingsFormat = PostingsFormat.forName(indexInput.readString());
            delegate = delegatePostingsFormat.fieldsProducer(state);
            IOUtils.close(indexInput);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public Iterator<String> iterator() {
            return delegate.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return new CachingTerms(delegate.terms(field), state.segmentInfo.name);
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
            private final String name;

            public CachingTerms(Terms delegate, String name) {
                this.delegate = delegate;
                this.name = name;
            }

            @Override
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
                TermsEnum termsEnum = cachedTermsEnums.get().get(name);
                if (termsEnum != null) {
                    return termsEnum;
                }
                termsEnum = delegate.iterator(reuse);
                cachedTermsEnums.get().put(name, termsEnum);
                return termsEnum;
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

        }

    }
}
