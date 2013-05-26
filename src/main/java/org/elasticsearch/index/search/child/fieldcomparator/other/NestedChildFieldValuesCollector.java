package org.elasticsearch.index.search.child.fieldcomparator.other;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.search.child.fieldcomparator.AbstractChildFieldComparator;
import org.elasticsearch.index.search.child.fieldcomparator.AbstractChildFieldValuesCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public class NestedChildFieldValuesCollector extends AbstractChildFieldValuesCollector {

    private final AbstractChildFieldValuesCollector wrapped;
    private final Filter rootDocumentsFilter;
    private final Filter innerDocumentsFilter;

    private FixedBitSet rootDocuments;
    private FixedBitSet innerDocuments;

    public NestedChildFieldValuesCollector(String parentType, SearchContext context, AbstractChildFieldValuesCollector wrapped, Filter rootDocumentsFilter, Filter innerDocumentsFilter) {
        super(parentType, context);
        this.wrapped = wrapped;
        this.rootDocumentsFilter = rootDocumentsFilter;
        this.innerDocumentsFilter = innerDocumentsFilter;
    }

    @Override
    public AbstractChildFieldComparator getFieldComparator(int numHits, int sortPos, boolean reversed) {
        return wrapped.getFieldComparator(numHits, sortPos, reversed);
    }

    @Override
    public void collect(int rootDoc, HashedBytesArray parentId) throws IOException {
        if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
            return;
        }

        int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
        int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
        if (nestedDoc >= rootDoc || nestedDoc == -1) {
            return;
        }
        do {
            wrapped.collect(nestedDoc, parentId);
            nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
        } while (nestedDoc >= rootDoc || nestedDoc == -1);
    }

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        super.setNextReader(readerContext);
        DocIdSet innerDocuments = innerDocumentsFilter.getDocIdSet(readerContext, null);
        if (DocIdSets.isEmpty(innerDocuments)) {
            this.innerDocuments = null;
        } else if (innerDocuments instanceof FixedBitSet) {
            this.innerDocuments = (FixedBitSet) innerDocuments;
        } else {
            this.innerDocuments = DocIdSets.toFixedBitSet(innerDocuments.iterator(), readerContext.reader().maxDoc());
        }
        DocIdSet rootDocuments = rootDocumentsFilter.getDocIdSet(readerContext, null);
        if (DocIdSets.isEmpty(rootDocuments)) {
            this.rootDocuments = null;
        } else if (rootDocuments instanceof FixedBitSet) {
            this.rootDocuments = (FixedBitSet) rootDocuments;
        } else {
            this.rootDocuments = DocIdSets.toFixedBitSet(rootDocuments.iterator(), readerContext.reader().maxDoc());
        }
        wrapped.setNextReader(readerContext);
    }
}
