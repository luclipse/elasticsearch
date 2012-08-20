package org.elasticsearch.index.field.data1.strings;

import org.elasticsearch.index.field.data1.FieldData;

/**
 *
 */
public interface StringFieldData extends FieldData {

    String value(int docId);

    String[] values(int docId);

}
