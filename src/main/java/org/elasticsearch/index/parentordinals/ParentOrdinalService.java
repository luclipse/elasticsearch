package org.elasticsearch.index.parentordinals;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.UTF8SortedAsUnicodeComparator;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

/**
 */
public abstract class ParentOrdinalService extends AbstractIndexComponent implements SegmentReader.CoreClosedListener {

    private final MapperService mapperService;
    protected final ConcurrentMap<Object, Map<String, ParentOrdinals>> readerToTypes;

    protected ParentOrdinalService(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings);
        this.mapperService = mapperService;
        this.readerToTypes = ConcurrentCollections.newConcurrentMap();
    }

    public final void refresh(IndicesWarmer.WarmerContext context) throws IOException {
        IndexReader indexReader = context.completeSearcher() != null ? context.completeSearcher().reader() : context.newSearcher().reader();
        if (!refreshNeeded(indexReader.leaves())) {
            return;
        }

        // We don't want to load uid of child documents, this allows us to not load uids of child types.
        NavigableSet<HashedBytesArray> parentTypes = new TreeSet<HashedBytesArray>(UTF8SortedAsUnicodeComparator.utf8SortedAsUnicodeSortOrder);
        BytesRef spare = new BytesRef();
        for (String type : mapperService.types()) {
            ParentFieldMapper parentFieldMapper = mapperService.documentMapper(type).parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentTypes.add(new HashedBytesArray(Strings.toUTF8Bytes(parentFieldMapper.type(), spare)));
            }
        }
        doRefresh(context, parentTypes);
    }

    protected abstract void doRefresh(IndicesWarmer.WarmerContext context, NavigableSet<HashedBytesArray> parentTypes) throws IOException;

    public final ParentOrdinals ordinals(String type, AtomicReaderContext context) throws IOException {
        AtomicReader reader = context.reader();
        Map<String, ParentOrdinals> segmentValues = readerToTypes.get(reader.getCoreCacheKey());
        if (segmentValues == null) {
            return null;
        }

        return segmentValues.get(type);
    }

    public boolean supportsValueLookup() {
        return false;
    }

    public BytesRef parentValue(int ordinal, BytesRef spare) {
        return null;
    }

    private boolean refreshNeeded(List<AtomicReaderContext> atomicReaderContexts) {
        for (AtomicReaderContext atomicReaderContext : atomicReaderContexts) {
            if (!readerToTypes.containsKey(atomicReaderContext.reader().getCoreCacheKey())) {
                return true;
            }
        }
        return false;
    }

}
