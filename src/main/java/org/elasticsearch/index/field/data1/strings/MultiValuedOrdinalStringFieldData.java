package org.elasticsearch.index.field.data1.strings;

/**
 *
 */
public interface MultiValuedOrdinalStringFieldData extends StringFieldData {

    int[] ordinals(int docId);

}
