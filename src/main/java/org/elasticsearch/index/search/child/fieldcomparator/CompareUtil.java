package org.elasticsearch.index.search.child.fieldcomparator;

import org.apache.lucene.util.BytesRef;

/**
 */
public final class CompareUtil {

    private CompareUtil() {
    }

    public static final int compareByte(byte left, byte right) {
        return left - right;
    }

    public static final int compareShort(short left, short right) {
        return left - right;
    }

    public static final int compareInt(int left, int right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    public static final int compareLng(long left, long right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    public static final int compareFloat(float left, float right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    public static final int compareDouble(double left, double right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    public static final int compareBytesRef(BytesRef val1, BytesRef val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

}
