package org.elasticsearch.index.search.child.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 */
public abstract class AbstractChildFieldComparator <T> extends FieldComparator<T> {

    private final String parentType;
    private final SearchContext searchContext;
    private IdReaderTypeCache idCache;

    protected AbstractChildFieldComparator(String parentType, SearchContext searchContext) {
        this.parentType = parentType;
        this.searchContext = searchContext;
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        HashedBytesArray uid = idCache.idByDoc(doc);
        return compareBottom(uid);
    }

    protected abstract int compareBottom(HashedBytesArray uid);

    @Override
    public void copy(int slot, int doc) throws IOException {
        HashedBytesArray uid = idCache.idByDoc(doc);
        copy(slot, uid);
    }

    protected abstract void copy(int slot, HashedBytesArray uid);

    @Override
    public FieldComparator<T> setNextReader(AtomicReaderContext readerContext) throws IOException {
        idCache = searchContext.idCache().reader(readerContext.reader()).type(parentType);
        return this;
    }

    @Override
    public int compareDocToValue(int doc, T value) throws IOException {
        HashedBytesArray uid = idCache.idByDoc(doc);
        return compareDocToValue(uid, value);
    }

    protected abstract int compareDocToValue(HashedBytesArray uid, T value);
}
