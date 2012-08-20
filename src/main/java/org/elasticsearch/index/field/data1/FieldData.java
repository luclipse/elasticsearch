package org.elasticsearch.index.field.data1;

/**
 *
 */
public interface FieldData {

    String fieldName();

    boolean hasValue(int docId);

    long computeSizeInBytes();

}
