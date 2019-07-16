package com.yaoyu;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yaoyu  2019/7/16 - 17:23
 */

// 1.实现Writable接口
public class FlowBean implements Writable {
    
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    
    // 2.反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        
        super();
        
    }
    
    public FlowBean(long upFlow, long downFlow) {
        
        super();
        
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow; 
        
    }

    // 3.写序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
        
    }
    
    // 4.写反序列化方法
    // 5.反序列化读取顺序必须和序列化方法的顺序一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
        
    }

    // 6.编写toString方法，方便后续打印到文本
    @Override
    public String toString() {
        
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
        
    }


    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
























