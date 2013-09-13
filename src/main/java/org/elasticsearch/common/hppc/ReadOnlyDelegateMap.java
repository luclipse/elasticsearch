package org.elasticsearch.common.hppc;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 */
public class ReadOnlyDelegateMap<K, V> implements Map<K, V> {

    private final ObjectObjectOpenHashMap<K, V> map;

    ReadOnlyDelegateMap(ObjectObjectOpenHashMap<K, V> map) {
        this.map = map;
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
    public boolean containsKey(Object key) {
        return map.containsKey((K) key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.values().contains((V) value);
    }

    @Override
    public V get(Object key) {
        return map.get((K) key);
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        final ObjectObjectOpenHashMap.KeysContainer keys = map.keys();
        return new Set<K>() {

            @Override
            public int size() {
                return keys.size();
            }

            @Override
            public boolean isEmpty() {
                return keys.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return keys.contains(o);
            }

            @Override
            public Iterator<K> iterator() {
                final Iterator<ObjectCursor<K>> iterator = keys.iterator();
                return new Iterator<K>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public K next() {
                        return iterator.next().value;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                return keys.toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return (T[]) toArray();
            }

            @Override
            public boolean add(K k) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                for (Object o : c) {
                    if (!keys.contains(o)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends K> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Collection<V> values() {
        final ObjectContainer<V> values = map.values();
        return new Collection<V>() {
            @Override
            public int size() {
                return values.size();
            }

            @Override
            public boolean isEmpty() {
                return values.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return values.contains((V) o);
            }

            @Override
            public Iterator<V> iterator() {
                final Iterator<ObjectCursor<V>> iterator = values.iterator();
                return new Iterator<V>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public V next() {
                        return iterator.next().value;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                return values.toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return (T[]) toArray();
            }

            @Override
            public boolean add(V v) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                for (Object o : c) {
                    if (!values.contains((V) o)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends V> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
