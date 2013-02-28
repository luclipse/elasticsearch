package org.elasticsearch.search.grouping;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;

/**
 */
public class GroupingBuilder implements ToXContent {

    private String groupField;
    private SortBuilder sortWithinGroup;
    private Integer sizeWithinGroup;
    private Integer offsetWithinGroup;

    public GroupingBuilder setGroupField(String groupField) {
        this.groupField = groupField;
        return this;
    }

    public GroupingBuilder setSortWithinGroup(SortBuilder sortWithinGroup) {
        this.sortWithinGroup = sortWithinGroup;
        return this;
    }

    public GroupingBuilder setSizeWithinGroup(int sizeWithinGroup) {
        this.sizeWithinGroup = sizeWithinGroup;
        return this;
    }

    public GroupingBuilder setOffsetWithinGroup(int offsetWithinGroup) {
        this.offsetWithinGroup = offsetWithinGroup;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("group");
        if (groupField != null) {
            builder.field("field", groupField);
        }
        if (sortWithinGroup != null) {
            builder.startObject("sort_within_group");
            sortWithinGroup.toXContent(builder, params);
            builder.endObject();
        }
        if (sizeWithinGroup != null) {
            builder.field("size", sizeWithinGroup);
        }
        if (offsetWithinGroup != null) {
            builder.field("offset", offsetWithinGroup);
        }
        return builder.endObject();
    }
}
