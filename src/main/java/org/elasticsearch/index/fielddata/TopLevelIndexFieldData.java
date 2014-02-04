package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.global.GlobalReference;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

/**
 */
public abstract class TopLevelIndexFieldData<GR extends GlobalReference> implements BaseFieldData {

    private final Index index;
    private final FieldMapper.Names fieldNames;
    protected final FieldDataType fieldDataType;
    protected final CircuitBreakerService breakerService;

    private IndexReader current;
    private GR reference;

    public TopLevelIndexFieldData(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, CircuitBreakerService breakerService) {
        this.index = index;
        this.fieldNames = fieldNames;
        this.fieldDataType = fieldDataType;
        this.breakerService = breakerService;
    }

    @Override
    public FieldMapper.Names getFieldNames() {
        return fieldNames;
    }

    @Override
    public void clear() {
        current = null;
        reference = null;
    }

    @Override
    public void clear(IndexReader reader) {
        if (reader.equals(current)) {
            reference = null;
        }
    }

    @Override
    public Index index() {
        return index;
    }

    /**
     * Loads the atomic field data for the top level reader, possibly cached.
     */
    public GR load(IndexReader indexReader) throws Exception {
        if (indexReader == current) {
            if (reference != null) {
                return reference;
            }
        }

        synchronized (this) {
            if (indexReader == current) {
                if (reference != null) {
                    return reference;
                } else {
                    return reference = loadDirect(indexReader, reference);
                }
            } else {
                current = indexReader;
                return reference = loadDirect(indexReader, reference);
            }
        }
    }

    /**
     * Loads directly the atomic field data for the top level reader, ignoring any caching involved.
     */
    public abstract GR loadDirect(IndexReader context, GR previous) throws Exception;

}
