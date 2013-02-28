package org.elasticsearch.index.search.grouping;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;

/**
 */
public class XTermFirstPassGroupingCollector extends AbstractFirstPassGroupingCollector<BytesRef> {

    private final IndexFieldData.WithOrdinals indexFieldData;
    private BytesValues.WithOrdinals values;
    private Ordinals.Docs ordinals;

    public XTermFirstPassGroupingCollector(Sort groupSort, int topNGroups, IndexFieldData.WithOrdinals indexFieldData) throws IOException {
        super(groupSort, topNGroups);
        this.indexFieldData = indexFieldData;
    }

    @Override
    protected BytesRef getDocGroupValue(int doc) {
        final int ord = ordinals.getOrd(doc);
        if (ord == 0) {
            return null;
        } else {
            return values.getValueByOrd(ord);
        }
    }

    @Override
    protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
        if (groupValue == null) {
            return null;
        } else if (reuse != null) {
            reuse.copyBytes(groupValue);
            return reuse;
        } else {
            return BytesRef.deepCopyOf(groupValue);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        super.setNextReader(readerContext);
        AtomicFieldData.WithOrdinals atomicFieldData = indexFieldData.load(readerContext);
        values = atomicFieldData.getBytesValues();
        ordinals = atomicFieldData.getBytesValues().ordinals();
    }
}
