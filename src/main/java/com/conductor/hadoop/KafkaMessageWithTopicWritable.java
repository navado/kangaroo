package com.conductor.hadoop;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom Writable implementation for Kafka MEssage with Topic information.
 * This is simple Custom Writable, and does not implement Comparable or RawComparator
 */
public class KafkaMessageWithTopicWritable implements Writable, WritableComparable<KafkaMessageWithTopicWritable> {

    // topic id
    private Text topicId;

    // Message content
    private BytesWritable content;

    public KafkaMessageWithTopicWritable() {
        this.topicId = new Text();
        this.content = new BytesWritable();
    }

    public KafkaMessageWithTopicWritable(Text topicId, BytesWritable content) {
        this.topicId = topicId;
        this.content = content;
    }

    public void write(DataOutput dataOutput) throws IOException {
        topicId.write(dataOutput);
        content.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        topicId.readFields(dataInput);
        content.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        // This is used by HashPartitioner
        return topicId.hashCode();
    }

    @Override
    public int compareTo(KafkaMessageWithTopicWritable o)
    {
        if (topicId.compareTo(o.topicId) == 0) {
            return (content.compareTo(o.content));
        }
        else return (topicId.compareTo(o.topicId));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof KafkaMessageWithTopicWritable){
            KafkaMessageWithTopicWritable other = (KafkaMessageWithTopicWritable) o;
            return topicId.equals(other.topicId) && content.equals(other.content);
        }
        return false;
    }

    public Text getTopicId() {
        return topicId;
    }

    public void setTopicId(Text topicId) {
        this.topicId = topicId;
    }

    public BytesWritable getContent() {
        return content;
    }

    public void setContent(BytesWritable content) {
        this.content = content;
    }
}