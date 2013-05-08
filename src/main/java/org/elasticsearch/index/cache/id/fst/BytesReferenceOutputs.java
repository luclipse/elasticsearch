package org.elasticsearch.index.cache.id.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.Outputs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;

/**
 */
public class BytesReferenceOutputs extends Outputs<BytesReference> {

    private final static BytesReference NO_OUTPUT = BytesArray.EMPTY;
    private final static BytesReferenceOutputs singleton = new BytesReferenceOutputs();

    private BytesReferenceOutputs() {
    }

    public static BytesReferenceOutputs getSingleton() {
        return singleton;
    }

    @Override
    public BytesReference common(BytesReference output1, BytesReference output2) {
        assert output1 != null;
        assert output2 != null;

        int pos1 = output1.arrayOffset();
        int pos2 = output2.arrayOffset();
        int stopAt1 = pos1 + Math.min(output1.length(), output2.length());
        while(pos1 < stopAt1) {
            if (output1.array()[pos1] != output2.array()[pos2]) {
                break;
            }
            pos1++;
            pos2++;
        }

        if (pos1 == output1.arrayOffset()) {
            // no common prefix
            return NO_OUTPUT;
        } else if (pos1 == output1.arrayOffset() + output1.length()) {
            // output1 is a prefix of output2
            return output1;
        } else if (pos2 == output2.arrayOffset() + output2.length()) {
            // output2 is a prefix of output1
            return output2;
        } else {
            return output1.slice(output1.arrayOffset(), pos1 - output1.arrayOffset());
        }
    }

    @Override
    public BytesReference subtract(BytesReference output, BytesReference inc) {
        assert output != null;
        assert inc != null;
        if (inc == NO_OUTPUT) {
            // no prefix removed
            return output;
        } else if (inc.length() == output.length()) {
            // entire output removed
            return NO_OUTPUT;
        } else {
            assert inc.length() < output.length(): "inc.length=" + inc.length() + " vs output.length=" + output.length();
            assert inc.length() > 0;
            return new BytesArray(output.array(), output.arrayOffset() + inc.length(), output.length() - inc.length());
        }
    }

    @Override
    public BytesReference add(BytesReference prefix, BytesReference output) {
        assert prefix != null;
        assert output != null;
        if (prefix == NO_OUTPUT) {
            return output;
        } else if (output == NO_OUTPUT) {
            return prefix;
        } else {
            assert prefix.length() > 0;
            assert output.length() > 0;
            BytesRef result = new BytesRef(prefix.length() + output.length());
            System.arraycopy(prefix.array(), prefix.arrayOffset(), result.bytes, 0, prefix.length());
            System.arraycopy(output.array(), output.arrayOffset(), result.bytes, prefix.length(), output.length());
            result.length = prefix.length() + output.length();
            return new BytesArray(result);
        }
    }

    @Override
    public void write(BytesReference prefix, DataOutput out) throws IOException {
        assert prefix != null;
        out.writeVInt(prefix.length());
        out.writeBytes(prefix.array(), prefix.arrayOffset(), prefix.length());
    }

    @Override
    public BytesReference read(DataInput in) throws IOException {
        final int len = in.readVInt();
        if (len == 0) {
            return NO_OUTPUT;
        } else {
            final BytesRef output = new BytesRef(len);
            in.readBytes(output.bytes, 0, len);
            output.length = len;
            return new BytesArray(output);
        }
    }

    @Override
    public BytesReference getNoOutput() {
        return NO_OUTPUT;
    }

    @Override
    public String outputToString(BytesReference output) {
        return output.toString();
    }
}
