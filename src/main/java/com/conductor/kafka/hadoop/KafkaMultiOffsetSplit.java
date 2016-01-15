package com.conductor.kafka.hadoop;

import com.conductor.kafka.Partition;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Special case of split that should cover several offsets.
 */
public class KafkaMultiOffsetSplit extends KafkaInputSplit {

    /** Flag, indicates the enf of split is reached */
    public static final long SPLIT_EOF = -1;

    /** All offsets length */
    private int offsetsLength = 0;

    /** for split covering several offsets */
    private List<Long> offsets;

    /** Current offset index */
    private int currentOffsetIndex = -1;

    /**
     * The {@link Writable} constructor; use {@link #KafkaMultiOffsetSplit(Partition, long, long, boolean, List<Long>, String)}.
     */
    public KafkaMultiOffsetSplit() {
    }

    public KafkaMultiOffsetSplit(final Partition partition, final long startOffset, final long endOffset,
                           final boolean partitionCommitter, List<Long> offsets, String location) {
        super(partition, startOffset, endOffset, partitionCommitter, location);

        // initialize offsets covered by split
        this.offsets = new ArrayList<>(offsets);
        // initialize offsets length
        this.offsetsLength = offsets.size();
        // set the current offset index
        this.currentOffsetIndex = this.offsetsLength > 0 ? 0 : -1;
        this.location = location;
    }

    public int getCurrentOffsetIndex() {
        return this.currentOffsetIndex;
    }

    public long getCurrentOffsetUpperLimit() {
        return this.currentOffsetIndex == this.offsetsLength - 1 ? this.offsets.get(this.currentOffsetIndex) : this.offsets.get(this.currentOffsetIndex + 1);
    }

    /**
     * Shift the current offset to next one covered by this split
     * @return next offset or SPLIT_EOF if the split is completed
     */
    public long shiftOffset() {
        if(this.currentOffsetIndex == this.offsetsLength - 1)
            return SPLIT_EOF;
        this.currentOffsetIndex += 1;
        return this.offsets.get(this.currentOffsetIndex);
    }

    public List<Long> getOffsets() {
        return offsets;
    }

    public void setOffsets(List<Long> offsets) {
        this.offsets = offsets;
    }

    public Integer getOffsetsLength() {
        return offsetsLength;
    }

    public void setOffsetsLength(Integer offsetsLength) {
        this.offsetsLength = offsetsLength;
    }

    public void setCurrentOffsetIndex(int currentOffsetIndex) {
        this.currentOffsetIndex = currentOffsetIndex;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.offsetsLength = in.readInt();
        this.offsets = new ArrayList<>();

        for (int i = 0; i < this.offsetsLength; ++i) {
            this.offsets.add(in.readLong());
        }
        this.currentOffsetIndex = in.readInt();

        this.partition = new Partition();
        this.partition.readFields(in);
        this.startOffset = in.readLong();
        this.endOffset = in.readLong();
        this.partitionCommitter = in.readBoolean();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.offsetsLength);
        for (int i = 0; i < this.offsetsLength; ++i) {
            out.writeLong(this.offsets.get(i));
        }
        out.writeInt(this.currentOffsetIndex);
        this.partition.write(out);
        out.writeLong(startOffset);
        out.writeLong(endOffset);
        out.writeBoolean(partitionCommitter);
    }

    @Override
    public int hashCode() {
        int result = (offsetsLength ^ (offsetsLength >>> 16));
        result = 31 * result + (offsets != null ? offsets.hashCode() : 0);
        result = 31 * result + super.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof KafkaMultiOffsetSplit))
            return false;

        final KafkaMultiOffsetSplit that = (KafkaMultiOffsetSplit) o;

        return offsetsLength == that.offsetsLength && super.equals(o);
    }
}
