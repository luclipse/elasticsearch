package org.elasticsearch.common.hppc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.hppc.predicates.ObjectPredicate;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;
import com.carrotsearch.hppc.procedures.ObjectProcedure;

import java.util.Iterator;

/**
 */
public class ImmutableObjectObjectMap<K, V> implements ObjectObjectMap<K, V> {

    private final ObjectObjectOpenHashMap<K, V> map;

    ImmutableObjectObjectMap(ObjectObjectOpenHashMap<K, V> map) {
        this.map = map;
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public int putAll(ObjectObjectAssociativeContainer<? extends K, ? extends V> container) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int putAll(Iterable<? extends ObjectObjectCursor<? extends K, ? extends V>> iterable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<ObjectObjectCursor<K, V>> iterator() {
        // no need to wrap, Iterator#remove() isn't implemented
        return map.iterator();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public int removeAll(ObjectContainer<? extends K> container) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int removeAll(ObjectPredicate<? super K> predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends ObjectObjectProcedure<? super K, ? super V>> T forEach(T procedure) {
        return map.forEach(procedure);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectCollection<K> keys() {
        return new ObjectCollection<K>() {

            final ObjectCollection<K> keys = map.keys();

            @Override
            public int removeAllOccurrences(K e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int removeAll(ObjectLookupContainer<? extends K> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int removeAll(ObjectPredicate<? super K> predicate) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int retainAll(ObjectLookupContainer<? extends K> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int retainAll(ObjectPredicate<? super K> predicate) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<ObjectCursor<K>> iterator() {
                return null;
            }

            @Override
            public boolean contains(K e) {
                return keys.contains(e);
            }

            @Override
            public int size() {
                return keys.size();
            }

            @Override
            public boolean isEmpty() {
                return keys.isEmpty();
            }

            @Override
            public K[] toArray(Class<? super K> clazz) {
                return keys.toArray(clazz);
            }

            @Override
            public Object[] toArray() {
                return keys.toArray();
            }

            @Override
            public <T extends ObjectProcedure<? super K>> T forEach(T procedure) {
                return keys.forEach(procedure);
            }

            @Override
            public <T extends ObjectPredicate<? super K>> T forEach(T predicate) {
                return keys.forEach(predicate);
            }
        };
    }

    @Override
    public ObjectContainer<V> values() {
        return new ObjectCollection<V>() {

            final ObjectContainer<V> values = map.values();

            @Override
            public int removeAllOccurrences(Object e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int removeAll(ObjectLookupContainer c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int removeAll(ObjectPredicate predicate) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int retainAll(ObjectLookupContainer c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int retainAll(ObjectPredicate predicate) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<ObjectCursor<V>> iterator() {
                return values.iterator();
            }

            @Override
            public boolean contains(V e) {
                return values.contains(e);
            }

            @Override
            public int size() {
                return values.size();
            }

            @Override
            public boolean isEmpty() {
                return values.isEmpty();
            }

            @Override
            public V[] toArray(Class<? super V> clazz) {
                return values.toArray(clazz);
            }

            @Override
            public Object[] toArray() {
                return values.toArray();
            }

            @Override
            public <T extends ObjectProcedure<? super V>> T forEach(T procedure) {
                return values.forEach(procedure);
            }

            @Override
            public <T extends ObjectPredicate<? super V>> T forEach(T predicate) {
                return values.forEach(predicate);
            }
        };
    }

}
