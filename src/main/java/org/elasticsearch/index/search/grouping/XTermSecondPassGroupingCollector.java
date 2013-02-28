package org.elasticsearch.index.search.grouping;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class XTermSecondPassGroupingCollector extends AbstractSecondPassGroupingCollector<BytesRef> {

    private final SentinelIntSet ordSet;
    private final IndexFieldData.WithOrdinals indexFieldData;

    private BytesValues.WithOrdinals values;
    private Ordinals.Docs ordinals;

    public XTermSecondPassGroupingCollector(Collection<SearchGroup<BytesRef>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields, IndexFieldData.WithOrdinals indexFieldData) throws IOException {
        super(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
        ordSet = new SentinelIntSet(groupMap.size(), -2);
        groupDocs = (SearchGroupDocs<BytesRef>[]) new SearchGroupDocs[ordSet.keys.length];
        this.indexFieldData = indexFieldData;
    }

    @Override
    protected SearchGroupDocs<BytesRef> retrieveGroup(int doc) throws IOException {
        int slot = ordSet.find(ordinals.getOrd(doc));
        if (slot >= 0) {
            return groupDocs[slot];
        }
        return null;
    }

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        super.setNextReader(readerContext);
        AtomicFieldData.WithOrdinals atomicFieldData = indexFieldData.load(readerContext);
        values = atomicFieldData.getBytesValues();
        ordinals = atomicFieldData.getBytesValues().ordinals();

        ordSet.clear();
        for (SearchGroupDocs<BytesRef> group : groupMap.values()) {
            int ord = group.groupValue == null ? 0 : binarySearch(group.groupValue, values, ordinals);
            if (ord >= 0) {
                groupDocs[ordSet.put(ord)] = group;
            }
        }
    }

    private final static int binarySearch(BytesRef term, BytesValues.WithOrdinals values, Ordinals.Docs ordinals) {
        int low = 0;
        int high = ordinals.getNumOrds();

        while (low <= high) {
            int mid = (low + high) >>> 1;
            BytesRef spare = values.getValueByOrd(mid);
            int cmp = spare.compareTo(term);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }
}
