package org.elasticsearch.index.field.data1.strings;

/**
 *
 */
public interface OrdinalStringFieldData extends StringFieldData {

    int ordinal(int docId);

}
