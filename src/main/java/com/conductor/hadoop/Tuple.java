package com.conductor.hadoop;
import kafka.message.MessageAndOffset;
import scala.collection.Iterator;

import javax.annotation.Resource;

public class Tuple {
    public String x = "";

    @Resource
    public Iterator<MessageAndOffset> y;

    public Tuple(){

    }
    public Tuple(String x, Iterator<MessageAndOffset> y) {
        this.x = x;
        this.y = y;
    }

    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public Iterator<MessageAndOffset> getY() {
        return y;
    }

    public void setY(Iterator<MessageAndOffset> y) {
        this.y = y;
    }
}
