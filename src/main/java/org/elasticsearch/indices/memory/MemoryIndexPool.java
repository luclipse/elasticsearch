package org.elasticsearch.indices.memory;

import org.apache.lucene.index.memory.ReusableMemoryIndex;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple {@link org.apache.lucene.index.memory.MemoryIndex} Pool that reuses MemoryIndex instance across threads and
 * allows each of the MemoryIndex instance to reuse its internal memory based on a user configured realtime value.
 */
final public class MemoryIndexPool extends AbstractComponent implements NodeSettingsService.Listener  {

    /**
     * Realtime index setting to control the number of MemoryIndex instances used to handle
     * Percolate requests. The default is <tt>10</tt>
     */
    public static final String PERCOLATE_POOL_SIZE = "indices.percolate.pool.size";

    /**
     * Realtime index setting to control the upper memory reuse limit across all {@link org.apache.lucene.index.memory.MemoryIndex} instances
     * pooled to handle Percolate requests. This is NOT a peak upper bound, percolate requests can use more memory than this upper
     * bound. Yet, if all pooled {@link org.apache.lucene.index.memory.MemoryIndex} instances are returned to the pool this marks the upper memory bound use
     * buy this idle instances. If more memory was allocated by a {@link org.apache.lucene.index.memory.MemoryIndex} the additinal memory is freed before it
     * returns to the pool. The default is <tt>1 MB</tt>
     */
    public static final String PERCOLATE_POOL_MAX_MEMORY = "indices.percolate.pool.reuse_memory_size";

    /**
     * Realtime index setting to control the timeout or the maximum waiting time
     * for an pooled memory index until an extra memory index is created. The default is <tt>100 ms</tt>
     */
    public static final String PERCOLATE_TIMEOUT = "indices.percolate.pool.timeout";

    private volatile BlockingQueue<ReusableMemoryIndex> memoryIndexQueue;

    // used to track the in-flight memoryIdx instances so we don't overallocate
    private int poolMaxSize;
    private int poolCurrentSize;
    private volatile long bytesPerMemoryIndex;
    private volatile ByteSizeValue maxMemorySize;
    private volatile TimeValue timeout;

    @Inject
    public MemoryIndexPool(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        poolMaxSize = settings.getAsInt(PERCOLATE_POOL_SIZE, 10);
        if (poolMaxSize <= 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_SIZE + " size must be > 0 but was [" + poolMaxSize + "]");
        }
        memoryIndexQueue = new ArrayBlockingQueue<ReusableMemoryIndex>(poolMaxSize);
        maxMemorySize = settings.getAsBytesSize(PERCOLATE_POOL_MAX_MEMORY, new ByteSizeValue(1, ByteSizeUnit.MB));
        if (maxMemorySize.bytes() < 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_MAX_MEMORY + " must be positive but was [" + maxMemorySize.bytes() + "]");
        }
        timeout = settings.getAsTime(PERCOLATE_TIMEOUT, new TimeValue(100));
        if (timeout.millis() < 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_TIMEOUT + " must be positive but was [" + timeout + "]");
        }
        bytesPerMemoryIndex = maxMemorySize.bytes() / poolMaxSize;
        nodeSettingsService.addListener(this);
    }

    public void onRefreshSettings(Settings settings) {
        int newPoolMaxSize = settings.getAsInt(PERCOLATE_POOL_SIZE, poolMaxSize);
        ByteSizeValue newMaxMemorySize = settings.getAsBytesSize(PERCOLATE_POOL_MAX_MEMORY, maxMemorySize);
        TimeValue newTimeout = settings.getAsTime(PERCOLATE_TIMEOUT, timeout);
        if (newTimeout.equals(timeout) && newPoolMaxSize == poolMaxSize && newMaxMemorySize.equals(maxMemorySize)) {
            return;
        }

        if (newTimeout.millis() < 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_TIMEOUT + " must be positive but was [" + timeout + "]");
        }
        if (!newTimeout.equals(timeout)) {
            logger.info("updating [{}] from [{}] to [{}]", PERCOLATE_TIMEOUT, this.timeout, newTimeout);
            timeout = newTimeout;
        }
        if (newPoolMaxSize == poolMaxSize && newMaxMemorySize.equals(maxMemorySize)) {
            return;
        }

        if (newMaxMemorySize.bytes() < 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_MAX_MEMORY + " must be positive but was [" + newMaxMemorySize.bytes() + "]");
        }
        if (newPoolMaxSize <= 0) {
            throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_SIZE + " size must be > 0 but was [" + newPoolMaxSize + "]");
        }

        synchronized (this) {
            if (!newMaxMemorySize.equals(maxMemorySize)) {
                logger.info("updating [{}] from [{}] to [{}]", PERCOLATE_POOL_MAX_MEMORY, this.maxMemorySize, newMaxMemorySize);
                maxMemorySize = newMaxMemorySize;
            }
            if (newPoolMaxSize != poolMaxSize) {
                logger.info("updating [{}] from [{}] to [{}]", PERCOLATE_POOL_SIZE, this.poolMaxSize, newPoolMaxSize);
                poolMaxSize = newPoolMaxSize;
            }
            poolCurrentSize = Integer.MAX_VALUE; // prevent new creations until we have the new index in place
            /*
             * if this has changed we simply change the blocking queue instance with a new pool
             * size and reset the
             */
            bytesPerMemoryIndex = newMaxMemorySize.bytes() / newPoolMaxSize;
            memoryIndexQueue = new ArrayBlockingQueue<ReusableMemoryIndex>(newPoolMaxSize);
            poolCurrentSize = 0; // lets refill the queue
        }
    }

    public ReusableMemoryIndex acquire() {
        final BlockingQueue<ReusableMemoryIndex> queue = memoryIndexQueue;
        final ReusableMemoryIndex poll = queue.poll();
        return poll == null ? waitOrCreate(queue) : poll;
    }

    private ReusableMemoryIndex waitOrCreate(BlockingQueue<ReusableMemoryIndex> queue) {
        synchronized (this) {
            if (poolCurrentSize < poolMaxSize) {
                poolCurrentSize++;
                return new ReusableMemoryIndex(false, bytesPerMemoryIndex);

            }
        }
        ReusableMemoryIndex poll = null;
        try {
            final TimeValue timeout = this.timeout; // only read the volatile var once
            poll = queue.poll(timeout.getMillis(), TimeUnit.MILLISECONDS); // delay this by 100ms by default
        } catch (InterruptedException ie) {
            // don't swallow the interrupt
            Thread.currentThread().interrupt();
        }
        return poll == null ? new ReusableMemoryIndex(false, bytesPerMemoryIndex) : poll;
    }

    public void release(ReusableMemoryIndex index) {
        assert index != null : "can't release null reference";
        if (bytesPerMemoryIndex == index.getMaxReuseBytes()) {
            index.reset();
            // only put is back into the queue if the size fits - prune old settings on the fly
            memoryIndexQueue.offer(index);
        }
    }
}
