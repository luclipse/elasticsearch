package org.elasticsearch.index.field.data1.strings;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldSelector;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;

/**
 *
 */
public class StoredSourceStringFieldData implements StringFieldData {

    private final IndexReader indexReader;
    private final String fieldName;

    public StoredSourceStringFieldData(String fieldName, IndexReader indexReader) {
        this.fieldName = fieldName;
        this.indexReader = indexReader;
    }

    public String value(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
            return SourceLookup.sourceAsMap(sourceField.getBinaryValue(), sourceField.getBinaryOffset(),
                    sourceField.getBinaryLength()).get(fieldName).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] values(int docId) {
        throw new UnsupportedOperationException();
    }

    public String fieldName() {
        return fieldName;
    }

    public boolean hasValue(int docId) {
        throw new UnsupportedOperationException();
    }

    public long computeSizeInBytes() {
        throw new UnsupportedOperationException();
    }

    public StoredSourceStringFieldData load(IndexReader reader, String fieldName) {
        return new StoredSourceStringFieldData(fieldName, reader);
    }

}
