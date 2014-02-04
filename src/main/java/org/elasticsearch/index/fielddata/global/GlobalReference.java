package org.elasticsearch.index.fielddata.global;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public interface GlobalReference {

    Ordinals ordinals(AtomicReaderContext context);

    BytesRef getValueByOrd(long ord);

}
