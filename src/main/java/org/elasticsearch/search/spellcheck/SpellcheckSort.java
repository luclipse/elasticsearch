package org.elasticsearch.search.spellcheck;

import org.elasticsearch.ElasticSearchException;

/**
 * Defines how spellcheck suggestion should be sorted.
 */
public enum SpellcheckSort {

    /**
     * Sort should first be based on score.
     */
    SCORE_FIRST((byte) 0x0),

    /**
     * Sort should first be based on document frequency.
     */
    FREQUENCY_FIRST((byte) 0x1);

    private byte id;

    private SpellcheckSort(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static SpellcheckSort fromId(byte id) {
        if (id == 0) {
            return SCORE_FIRST;
        } else if (id == 1) {
            return FREQUENCY_FIRST;
        } else {
            throw new ElasticSearchException("Illegal spellcheck sort " + id);
        }
    }

}
