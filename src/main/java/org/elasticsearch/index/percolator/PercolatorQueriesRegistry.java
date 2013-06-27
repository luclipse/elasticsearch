package org.elasticsearch.index.percolator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class PercolatorQueriesRegistry extends AbstractIndexShardComponent {

    // This is a shard level service, but these below are index level service:
    private final IndexQueryParserService queryParserService;
    private final MapperService mapperService;
    private final IndicesLifecycle indicesLifecycle;
    private final ShardIndexingService indexingService;

    private final Map<String, Query> percolateQueries = new HashMap<String, Query>();
    private final ShardLifecycleListener shardLifecycleListener = new ShardLifecycleListener();
    private final RealTimePercolatorOperationListener realTimePercolatorOperationListener = new RealTimePercolatorOperationListener();

    private final Object lock = new Object();
    private boolean initialQueriesFetchDone = false;

    @Inject
    public PercolatorQueriesRegistry(ShardId shardId, @IndexSettings Settings indexSettings, IndexQueryParserService queryParserService,
                                     ShardIndexingService indexingService, IndicesLifecycle indicesLifecycle, MapperService mapperService) {
        super(shardId, indexSettings);
        this.queryParserService = queryParserService;
        this.mapperService = mapperService;
        this.indicesLifecycle = indicesLifecycle;
        this.indexingService = indexingService;

        indicesLifecycle.addListener(shardLifecycleListener);
        indexingService.addListener(realTimePercolatorOperationListener);
    }

    public Map<String, Query> percolateQueries() {
        return percolateQueries;
    }

    public void addPercolateQuery(String uid, BytesReference source) {
        Query query = parseQuery(uid, source);
        synchronized (lock) {
            percolateQueries.put(uid, query);
        }
    }

    public void removePercolateQuery(String uid) {
        synchronized (lock) {
            percolateQueries.remove(uid);
        }
    }

    public void close() {
        indicesLifecycle.removeListener(shardLifecycleListener);
        indexingService.removeListener(realTimePercolatorOperationListener);
        percolateQueries.clear();
    }

    Query parseQuery(String uid, BytesReference source) {
        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            Query query = null;
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticSearchException("failed to parse query [" + uid + "], not starting with OBJECT");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        query = queryParserService.parse(parser).query();
                        break;
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
            return query;
        } catch (Exception e) {
            throw new ElasticSearchException("failed to parse query [" + uid + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private class ShardLifecycleListener extends IndicesLifecycle.Listener {

        private boolean hasPercolatorType(IndexShard indexShard) {
            ShardId otherShardId = indexShard.shardId();
            return shardId.equals(otherShardId) && mapperService.hasMapping(Percolator.Constants.TYPE_NAME);
        }

        private void loadQueries(IndexShard shard) {
            try {
                shard.refresh(new Engine.Refresh(true));
                Engine.Searcher searcher = shard.searcher();
                try {
                    // create a query to fetch all queries that are registered under the index name (which is the type
                    // in the percolator).
                    Query query = new XConstantScoreQuery(
                            // TODO: no push, cache it
                            new TermFilter(new Term(TypeFieldMapper.NAME, Percolator.Constants.TYPE_NAME))
                    );
                    QueriesLoaderCollector queries = new QueriesLoaderCollector(PercolatorQueriesRegistry.this);
                    searcher.searcher().search(query, queries);
                    percolateQueries.putAll(queries.queries());
                } finally {
                    searcher.release();
                }
            } catch (Exception e) {
                throw new PercolatorException(shardId.index(), "failed to load queries from percolator index", e);
            }
        }

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            // add a listener that will update based on changes done to the _percolate index
            // the relevant indices with loaded queries
            // TODO: no push
            // TODO: Only make realTimePercolatorOperationListener active when there is a _percolate type
//            if (indexShard.shardId().index().name().equals(INDEX_NAME)) {
//                indexShard.indexingService().addListener(realTimePercolatorOperationListener);
//            }
        }

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            if (hasPercolatorType(indexShard)) {
                // percolator index has started, fetch what we can from it and initialize the indices
                // we have
                synchronized (lock) {
                    if (initialQueriesFetchDone) {
                        return;
                    }
                    logger.debug("loading percolator queries for index [{}] and shard[{}]...", shardId.index(), shardId.id());
                    loadQueries(indexShard);
                    logger.trace("done loading percolator queries for index [{}] and shard[{}]", shardId.index(), shardId.id());
                    initialQueriesFetchDone = true;
                }
            }

            // TODO: Figure out why we check it twice???
            if (!hasPercolatorType(indexShard)) {
                return;
            }

            synchronized (lock) {
                if (initialQueriesFetchDone) {
                    return;
                }
                // we load queries for this index
                logger.debug("loading percolator queries for index [{}]...", shardId.index());
                loadQueries(indexShard);
                logger.trace("done loading percolator queries for index [{}]", shardId.index());
                initialQueriesFetchDone = true;
            }
        }
    }

    private class RealTimePercolatorOperationListener extends IndexingOperationListener {

        @Override
        public Engine.Create preCreate(Engine.Create create) {
            // validate the query here, before we index
            if (Percolator.Constants.TYPE_NAME.equals(create.type())) {
                parseQuery(create.id(), create.source());
            }
            return create;
        }

        @Override
        public void postCreateUnderLock(Engine.Create create) {
            // add the query under a doc lock
            if (Percolator.Constants.TYPE_NAME.equals(create.type())) {
                addPercolateQuery(create.id(), create.source());
            }
        }

        @Override
        public Engine.Index preIndex(Engine.Index index) {
            // validate the query here, before we index
            if (Percolator.Constants.TYPE_NAME.equals(index.type())) {
                parseQuery(index.id(), index.source());
            }
            return index;
        }

        @Override
        public void postIndexUnderLock(Engine.Index index) {
            // add the query under a doc lock
            if (Percolator.Constants.TYPE_NAME.equals(index.type())) {
                addPercolateQuery(index.id(), index.source());
            }
        }

        @Override
        public void postDeleteUnderLock(Engine.Delete delete) {
            // remove the query under a lock
            if (Percolator.Constants.TYPE_NAME.equals(delete.type())) {
                removePercolateQuery(delete.id());
            }
        }

        // TODO:
        @Override
        public Engine.DeleteByQuery preDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            return super.preDeleteByQuery(deleteByQuery);
        }

        // TODO:
        @Override
        public void postDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            super.postDeleteByQuery(deleteByQuery);
        }
    }

}
