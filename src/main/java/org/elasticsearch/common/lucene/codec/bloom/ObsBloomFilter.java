/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.codec.bloom;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.MurmurHash;
import org.elasticsearch.common.RamUsage;

import java.io.IOException;

public class ObsBloomFilter implements BloomFilter {

    private final int hashCount;

    private final OpenBitSet bitset;
    private final long size;

    ObsBloomFilter(int hashCount, long size) {
        this.hashCount = hashCount;
        this.bitset = new OpenBitSet(size);
        this.size = size;
    }
    ObsBloomFilter(int hashCount, OpenBitSet bitset) {
        this.hashCount = hashCount;
        this.bitset = bitset;
        this.size = bitset.size();
    }

    long emptyBuckets() {
        long n = 0;
        for (long i = 0; i < buckets(); i++) {
            if (!bitset.get(i)) {
                n++;
            }
        }
        return n;
    }

    private long buckets() {
        return size;
    }

    private long[] getHashBuckets(byte[] key, int offset, int length) {
        return getHashBuckets(key, offset, length, hashCount, buckets());
    }

    static long[] getHashBuckets(byte[] b, int offset, int length, int hashCount, long max) {
        long[] result = new long[hashCount];
        long[] hash = MurmurHash.hash3_x64_128(b, offset, length, 0L);
        for (int i = 0; i < hashCount; ++i) {
            result[i] = Math.abs((hash[0] + (long) i * hash[1]) % max);
        }
        return result;
    }

    public void add(BytesRef term) {
        // inline the hash buckets so we don't have to create the int[] each time...
        long[] hash = MurmurHash.hash3_x64_128(term.bytes, term.offset, term.length, 0L);
        for (int i = 0; i < hashCount; ++i) {
            long bucketIndex = Math.abs((hash[0] + (long) i * hash[1]) % size);
            bitset.fastSet(bucketIndex);
        }
    }

    @Override
    public boolean isPresent(BytesRef term) {
        // inline the hash buckets so we don't have to create the int[] each time...
        long[] hash = MurmurHash.hash3_x64_128(term.bytes, term.offset, term.length, 0L);
        for (int i = 0; i < hashCount; ++i) {
            long bucketIndex = Math.abs((hash[0] + (long) i * hash[1]) % size);
            if (!bitset.fastGet(bucketIndex)) {
                return false;
            }
        }
        return true;
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeVInt(hashCount);
        out.writeVInt(bitset.getNumWords());
        long[] bits = bitset.getBits();
        out.writeVInt(bits.length);
        for (long bit : bits) {
            out.writeLong(bit);
        }
    }

    public static ObsBloomFilter deSerialize(DataInput input) throws IOException {
        int hashCount = input.readVInt();
        int numWords = input.readVInt();
        int bitsLength = input.readVInt();
        long[] bits = new long[bitsLength];
        for (int i = 0; i < bitsLength; i++) {
            bits[i] = input.readLong();
        }
        OpenBitSet bitSet = new OpenBitSet(bits, numWords);
        return new ObsBloomFilter(hashCount, bitSet);
    }

    public void clear() {
        bitset.clear(0, bitset.size());
    }

    @Override
    public long sizeInBytes() {
        return bitset.getBits().length * RamUsage.NUM_BYTES_LONG + RamUsage.NUM_BYTES_ARRAY_HEADER + RamUsage.NUM_BYTES_INT /* wlen */;
    }
}
