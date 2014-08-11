/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import java.util.Iterator;

/**
 * Immutable set based on the hppc {@link ObjectOpenHashSet}.
 *
 * For a copy on write this immutable set is more efficient because of the {@link #builder(ImmutableOpenSet)} method
 * that uses cloning on array backing the wrapped set.
 */
public final class ImmutableOpenSet<KType> implements Iterable<ObjectCursor<KType>> {

    @SuppressWarnings("unchecked")
    private final static ImmutableOpenSet EMPTY = new ImmutableOpenSet(new ObjectOpenHashSet());

    private final ObjectOpenHashSet<KType> set;

    private ImmutableOpenSet(ObjectOpenHashSet<KType> set) {
        this.set = set;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<ObjectCursor<KType>> iterator() {
        return set.iterator();
    }

    /**
     * @see ObjectOpenHashSet#contains(Object)
     */
    public boolean contains(KType e) {
        return set.contains(e);
    }

    /**
     * @return an empty immutable set
     */
    @SuppressWarnings("unchecked")
    public static <T> ImmutableOpenSet<T> of() {
        return EMPTY;
    }

    /**
     * @return a builder to create an immutable set
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * @return a builder to create an immutable set and preset with the specified initial capacity.
     */
    public static <T> Builder<T> builder(int initialCapacity) {
        return new Builder<>(initialCapacity);
    }

    /**
     * @return a builder to create an immutable set based on existing immutable set.
     */
    public static <T> Builder<T> builder(ImmutableOpenSet<T> set) {
        return new Builder<>(set);
    }

    public final static class Builder<KType> {

        private ObjectOpenHashSet<KType> set;

        Builder() {
            //noinspection unchecked
            this(EMPTY);
        }

        Builder(int initialCapacity) {
            this.set = new ObjectOpenHashSet<>(initialCapacity);
        }

        Builder(ImmutableOpenSet<KType> set) {
            this.set = set.set.clone();
        }

        /**
         * @see ObjectOpenHashSet#add(Object)
         */
        public boolean add(KType k) {
            return set.add(k);
        }

        /**
         * @see ObjectOpenHashSet#remove(Object)
         */
        public boolean remove(KType key) {
            return set.remove(key);
        }

        /**
         * @return an immutable set with the elements added via this builder
         */
        public ImmutableOpenSet<KType> build() {
            ObjectOpenHashSet<KType> set = this.set;
            this.set = null;
            return new ImmutableOpenSet<>(set);
        }

    }
}
