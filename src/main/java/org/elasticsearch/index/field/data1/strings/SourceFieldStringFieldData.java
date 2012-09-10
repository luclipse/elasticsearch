package org.elasticsearch.index.field.data1.strings;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldSelector;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class SourceFieldStringFieldData implements StringFieldData {

    private final IndexReader indexReader;
    private final String fieldName;

    public SourceFieldStringFieldData(String fieldName, IndexReader indexReader) {
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
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
            List<String> values = (List<String>) SourceLookup.sourceAsMap(sourceField.getBinaryValue(), sourceField.getBinaryOffset(),
                    sourceField.getBinaryLength()).get(fieldName);
            return values.toArray(new String[values.size()]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String fieldName() {
        return fieldName;
    }

    public boolean hasValue(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            return doc.getFieldable(SourceFieldMapper.NAME) != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long computeSizeInBytes() {
        return 0L;
    }

    public SourceFieldStringFieldData load(IndexReader reader, String fieldName) {
        return new SourceFieldStringFieldData(fieldName, reader);
    }

}
