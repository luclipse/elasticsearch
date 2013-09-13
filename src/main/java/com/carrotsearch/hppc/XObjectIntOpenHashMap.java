package com.carrotsearch.hppc;

import java.util.Arrays;

import static com.carrotsearch.hppc.HashContainerUtils.nextCapacity;
import static com.carrotsearch.hppc.Internals.rehash;

/**
 */
public class XObjectIntOpenHashMap<KType> extends ObjectIntOpenHashMap<KType> {

    public final int noEntryValue;

    public XObjectIntOpenHashMap(int initialCapacity, int noEntryValue) {
        super(initialCapacity);
        this.noEntryValue = noEntryValue;
        if (noEntryValue != 0) {
            Arrays.fill(values, noEntryValue);
        }
    }

    public XObjectIntOpenHashMap(int initialCapacity, float loadFactor, int noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        if (noEntryValue != 0) {
            Arrays.fill(values, noEntryValue);
        }
    }

    @Override
    public int get(KType key) {
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
        return noEntryValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int put(KType key, int value) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot])
        {
            if (((key) == null ? (keys[slot]) == null : (key).equals((keys[slot]))))
            {
                final int oldValue = values[slot];
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
        return ((int) 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int putOrAdd(KType key, int putValue, int additionValue) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot])
        {
            if (((key) == null ? (keys[slot]) == null : (key).equals((keys[slot]))))
            {
                return values[slot] = (int) (values[slot] + additionValue);
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
    private void expandAndPut(KType pendingKey, int pendingValue, int freeSlot) {
        assert assigned == resizeAt;
        assert !allocated[freeSlot];

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final KType   [] oldKeys      = this.keys;
        final int   [] oldValues    = this.values;
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
        final KType []   keys = this.keys;
        final int []   values = this.values;
        final boolean [] allocated = this.allocated;
        final int mask = allocated.length - 1;
        for (int i = oldAllocated.length; --i >= 0;)
        {
            if (oldAllocated[i])
            {
                final KType k = oldKeys[i];
                final int v = oldValues[i];

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
        KType [] keys = Internals.<KType[]>newArray(capacity);
        int [] values = new int [capacity];
        if (noEntryValue != 0) {
            Arrays.fill(values, noEntryValue);
        }
        boolean [] allocated = new boolean [capacity];

        this.keys = keys;
        this.values = values;
        this.allocated = allocated;

        this.resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        this.perturbation = computePerturbationValue(capacity);
    }

}
