package org.elasticsearch.common.hppc;

import com.carrotsearch.hppc.*;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

import java.util.Map;

/**
 */
public final class HppcMaps {

    private HppcMaps() {
    }

    /**
     * Returns a new map with the given initial capacity
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap(int capacity) {
        return new ObjectObjectOpenHashMap<K, V>(capacity);
    }

    /**
     * Returns a new map with a default initial capacity of
     * {@value com.carrotsearch.hppc.HashContainerUtils#DEFAULT_CAPACITY}
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap() {
        return newMap(16);
    }

    public static <K, V> ObjectObjectMap<K, V> immutable(ObjectObjectOpenHashMap<K, V> map) {
        return new ImmutableObjectObjectMap<K, V>(map);
    }

    public static <K, V> Map<K, V> wrap(final ObjectObjectOpenHashMap<K, V> map) {
        return new DelegateMap<K, V>(map);
    }

    public static <K, V> Map<K, V> wrapAndImmutable(ObjectObjectOpenHashMap<K, V> map) {
        return new ReadOnlyDelegateMap<K, V>(map);
    }

    /**
     * Returns a map like {@link #newMap()} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap() {
        return ensureNoNullKeys(16);
    }

    /**
     * Returns a map like {@link #newMap(int)} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap(int capacity) {
        return ensureNoNullKeys(capacity);
    }

    /**
     * Wraps the given map and prevent adding of <code>null</code> keys.
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> ensureNoNullKeys(int capacity) {
        return new ObjectObjectOpenHashMap<K, V>(capacity) {

            @Override
            public V put(K key, V value) {
                if (key == null) {
                    throw new ElasticSearchIllegalArgumentException("Map key must not be null");
                }
                return super.put(key, value);
            }

        };
    }

    public final static class Object {

        public final static class Integer {

            public static <V> XObjectIntOpenHashMap<V> newMapNoEntry(int noEntryValue) {
                return new XObjectIntOpenHashMap<V>(16, noEntryValue);
            }

            public static <V> ObjectIntOpenHashMap<V> ensureNoNullKeys(int capacity, float loadFactor, int noEntryValue) {
                return new XObjectIntOpenHashMap<V>(capacity, loadFactor, noEntryValue);
            }

        }

        public final static class Float {

            public static <V> ObjectFloatOpenHashMap<V> newMapNoEntry(float noEntryValue) {
                return new XObjectFloatOpenHashMap<V>(16, 0.75f, noEntryValue);
            }

        }

        public final static class Long {

            public static <K> ObjectLongOpenHashMap<K> newMapNoEntry(long noEntryValue) {
                return new XObjectLongOpenHashMap<K>(16, 0.75f, noEntryValue);
            }

        }

    }

}
