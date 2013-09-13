package com.carrotsearch.hppc;

import java.util.Arrays;

import static com.carrotsearch.hppc.HashContainerUtils.nextCapacity;
import static com.carrotsearch.hppc.Internals.rehash;

/**
 */
public class XObjectFloatOpenHashMap<K> extends ObjectFloatOpenHashMap<K> {

    private final float noEntryKey;

    public XObjectFloatOpenHashMap(int initialCapacity, float loadFactor, float noEntryKey) {
        super(initialCapacity, loadFactor);
        if (noEntryKey != 0) {
            Arrays.fill(values, noEntryKey);
        }
        this.noEntryKey = noEntryKey;
    }

    @Override
    public float get(K key) {
        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (allocated[slot])
        {
            if (((key) == null ? (keys[slot]) == null : (key).equals((keys[slot]))))
            {
                return values[slot];
            }

            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return noEntryKey;
    }

    @Override
    public float put(K key, float value) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot])
        {
            if (((key) == null ? (keys[slot]) == null : (key).equals((keys[slot]))))
            {
                final float oldValue = values[slot];
                values[slot] = value;
                return oldValue;
            }

            slot = (slot + 1) & mask;
        }

        // Check if we need to grow. If so, reallocate new data, fill in the last element
        // and rehash.
        if (assigned == resizeAt) {
            expandAndPut(key, value, slot);
        } else {
            assigned++;
            allocated[slot] = true;
            keys[slot] = key;
            values[slot] = value;
        }
        return ((float) 0);
    }

    public float putOrAdd(K key, float putValue, float additionValue) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot])
        {
            if (((key) == null ? (keys[slot]) == null : (key).equals((keys[slot]))))
            {
                return values[slot] = (float) (values[slot] + additionValue);
            }

            slot = (slot + 1) & mask;
        }

        if (assigned == resizeAt) {
            expandAndPut(key, putValue, slot);
        } else {
            assigned++;
            allocated[slot] = true;
            keys[slot] = key;
            values[slot] = putValue;
        }
        return putValue;
    }

    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndPut(K pendingKey, float pendingValue, int freeSlot) {
        assert assigned == resizeAt;
        assert !allocated[freeSlot];

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final K[] oldKeys      = this.keys;
        final float   [] oldValues    = this.values;
        final boolean [] oldAllocated = this.allocated;

        allocateBuffers(nextCapacity(keys.length));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        lastSlot = -1;
        assigned++;
        oldAllocated[freeSlot] = true;
        oldKeys[freeSlot] = pendingKey;
        oldValues[freeSlot] = pendingValue;

        // Rehash all stored keys into the new buffers.
        final K[]   keys = this.keys;
        final float []   values = this.values;
        final boolean [] allocated = this.allocated;
        final int mask = allocated.length - 1;
        for (int i = oldAllocated.length; --i >= 0;)
        {
            if (oldAllocated[i])
            {
                final K k = oldKeys[i];
                final float v = oldValues[i];

                int slot = rehash(k, perturbation) & mask;
                while (allocated[slot])
                {
                    slot = (slot + 1) & mask;
                }

                allocated[slot] = true;
                keys[slot] = k;
                values[slot] = v;
            }
        }

        /*  */ Arrays.fill(oldKeys,   null); /*  */
        /*  */
    }

    /**
     * Allocate internal buffers for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */
    private void allocateBuffers(int capacity) {
        K[] keys = Internals.<K[]>newArray(capacity);
        float [] values = new float [capacity];
        if (noEntryKey != 0) {
            Arrays.fill(values, noEntryKey);
        }
        boolean [] allocated = new boolean [capacity];

        this.keys = keys;
        this.values = values;
        this.allocated = allocated;

        this.resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        this.perturbation = computePerturbationValue(capacity);
    }

}
