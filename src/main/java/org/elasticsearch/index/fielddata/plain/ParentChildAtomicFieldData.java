package org.elasticsearch.index.fielddata.plain;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

/**
 */
public class ParentChildAtomicFieldData implements AtomicFieldData.WithOrdinals {

    private final ImmutableOpenMap<String, PagedBytesAtomicFieldData> typeToIds;
    private final long numberUniqueValues;
    private final long memorySizeInBytes;

    public ParentChildAtomicFieldData(ImmutableOpenMap<String, PagedBytesAtomicFieldData> typeToIds) {
        this.typeToIds = typeToIds;
        long numValues = 0;
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            numValues += cursor.value.getNumberUniqueValues();
        }
        this.numberUniqueValues = numValues;
        long size = 0;
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            size += cursor.value.getMemorySizeInBytes();
        }
        this.memorySizeInBytes = size;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public boolean isValuesOrdered() {
        return false;
    }

    @Override
    public int getNumDocs() {
        return typeToIds.isEmpty() ? 0 : typeToIds.values().toArray(PagedBytesAtomicFieldData.class)[0].getNumDocs();
    }

    @Override
    public long getNumberUniqueValues() {
        return numberUniqueValues;
    }

    @Override
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
        // Each PagedBytesAtomicFieldData typeToIds has its own ordinal numbering, so I can't merge that...
        // I can create a combined BytesValues impl simply merging the id values and create the ordinals on the fly?
        throw new UnsupportedOperationException();
    }

    public BytesValues.WithOrdinals getBytesValues(boolean needsHashes, String type) {
        WithOrdinals atomicFieldData = typeToIds.get(type);
        if (atomicFieldData != null) {
            return atomicFieldData.getBytesValues(needsHashes);
        } else {
            return null;
        }
    }

    @Override
    public ScriptDocValues getScriptValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            cursor.value.close();
        }
    }
}
