package org.elasticsearch.index.field.data1.strings.stored;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.field.data1.strings.SingleValuedStringFieldData;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldSelector;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SourceFieldSingleValuedStringFieldData implements SingleValuedStringFieldData {

    private final IndexReader indexReader;
    private final SourceFieldMapper sourceFieldMapper;
    private final String sourcePath;

    public SourceFieldSingleValuedStringFieldData(IndexReader indexReader, String sourcePath, SourceFieldMapper sourceFieldMapper) {
        this.sourceFieldMapper = sourceFieldMapper;
        this.sourcePath = sourcePath;
        this.indexReader = indexReader;
    }

    public BytesRef value(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            byte[] sourceData = sourceFieldMapper.value(doc);
            if (sourceData == null) {
                return null;
            }


            Tuple<XContentType, Map<String, Object>> sourceAsMap = XContentHelper.convertToMap(sourceData, false);
            List values = XContentMapValues.extractRawValues(sourcePath, sourceAsMap.v2());
            if (!values.isEmpty()) {
                return new BytesRef(values.get(0).toString());
            } else {
                return null;
            }
            // TODO:
            /*XContentParser parser = XContentHelper.createParser(sourceData, 0, sourceData.length);
            try {
                Deque<String> stack = new LinkedList<String>();
                for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                    switch (token) {
                        case START_ARRAY:
                            break;
                        case START_OBJECT:
                            break;
                        case END_OBJECT:
                            stack.removeLast();
                            break;
                        case FIELD_NAME:
                            stack.add(parser.currentName());
                            if (sourcePath.equals(path(stack))) {
                                return new BytesRef(parser.binaryValue());
                            }
                            break;
                    }
                }
                return null;
            } finally {
                parser.close();
            }*/
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String fieldName() {
        return sourcePath;
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

    public static SourceFieldSingleValuedStringFieldData load(IndexReader reader, StringFieldMapper fieldMapper, SourceFieldMapper sourceFieldMapper) {
        return new SourceFieldSingleValuedStringFieldData(reader, fieldMapper.names().sourcePath(),sourceFieldMapper);
    }

}
