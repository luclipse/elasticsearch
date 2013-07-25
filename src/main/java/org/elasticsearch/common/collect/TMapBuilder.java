/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.common.collect;

import gnu.trove.impl.Constants;
import gnu.trove.map.TMap;
import org.elasticsearch.common.trove.ExtTHashMap;

import java.util.Map;

/**
 */
public class TMapBuilder<K, V> {

    private TMap<K, V> map = new ExtTHashMap<K, V>(XMaps.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR);

    public static <K, V> TMapBuilder<K, V> newMapBuilder() {
        return new TMapBuilder<K, V>();
    }

    public static <K, V> TMapBuilder<K, V> newMapBuilder(TMap<K, V> map) {
        return new TMapBuilder<K, V>(map);
    }

    public TMapBuilder() {
    }

    public TMapBuilder(TMap<K, V> map) {
        this.map = map;
    }

    public TMapBuilder<K, V> putAll(Map<K, V> map) {
        this.map.putAll(map);
        return this;
    }

    public TMapBuilder<K, V> put(K key, V value) {
        this.map.put(key, value);
        return this;
    }

    public TMapBuilder<K, V> remove(K key) {
        this.map.remove(key);
        return this;
    }

    public TMapBuilder<K, V> clear() {
        this.map.clear();
        return this;
    }

    public V get(K key) {
        return map.get(key);
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public Map<K, V> map() {
        return this.map;
    }

    public Map<K, V> readOnlyMap() {
        return map;
//        return Collections.unmodifiableMap(map);
    }

}
