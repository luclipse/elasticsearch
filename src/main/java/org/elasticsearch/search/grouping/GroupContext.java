package org.elasticsearch.search.grouping;

import org.apache.lucene.search.Sort;

/**
 */
public class GroupContext {

    private String groupField;
    private Sort sortWithinGroup;
    private int sizeWithinGroup;
    private int offsetWithinGroup;

    public String groupField() {
        return groupField;
    }

    public void groupField(String groupField) {
        this.groupField = groupField;
    }

    public Sort sortWithinGroup() {
        return sortWithinGroup;
    }

    public void sortWithinGroup(Sort sortWithinGroup) {
        this.sortWithinGroup = sortWithinGroup;
    }

    public int sizeWithinGroup() {
        return sizeWithinGroup;
    }

    public void sizeWithinGroup(int sizeWithinGroup) {
        this.sizeWithinGroup = sizeWithinGroup;
    }

    public int offsetWithinGroup() {
        return offsetWithinGroup;
    }

    public void offsetWithinGroup(int offsetWithinGroup) {
        this.offsetWithinGroup = offsetWithinGroup;
    }
}
