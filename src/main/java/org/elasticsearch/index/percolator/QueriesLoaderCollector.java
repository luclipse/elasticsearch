package org.elasticsearch.index.percolator;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;

import java.io.IOException;
import java.util.Map;

/**
 */
final class QueriesLoaderCollector extends Collector {

    private final Map<String, Query> queries = Maps.newHashMap();
    private final PercolatorQueriesRegistry percolator;

    private AtomicReader reader;

    QueriesLoaderCollector(PercolatorQueriesRegistry percolator) {
        this.percolator = percolator;
    }

    public Map<String, Query> queries() {
        return this.queries;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        // the _source is the query
        UidAndSourceFieldsVisitor fieldsVisitor = new UidAndSourceFieldsVisitor();
        reader.document(doc, fieldsVisitor);
        String id = fieldsVisitor.uid().id();
        try {
            final Query parseQuery = percolator.parseQuery(id, fieldsVisitor.source());
            if (parseQuery != null) {
                queries.put(id, parseQuery);
            } else {
                // TODO: no push
                System.err.println("failed to add query");
//                logger.warn("failed to add query [{}] - parser returned null", id);
            }

        } catch (Exception e) {
            // TODO: no push
            e.printStackTrace();
//            logger.warn("failed to add query [{}]", e, id);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.reader = context.reader();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
