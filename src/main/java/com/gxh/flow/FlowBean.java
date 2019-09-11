package com.gxh.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upflow;
    private long downflow;
    private long sumflow;

    public FlowBean() {
    }

    public void set(long upflow,long downflow){
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow+downflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    /**
     *
     * 序列化方法
     * @param out 框架给我们提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumflow);
    }

    /**
     * 反序列化方法
     * @param in 框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upflow = in.readLong();
        downflow = in.readLong();
        sumflow = in.readLong();
    }
}
