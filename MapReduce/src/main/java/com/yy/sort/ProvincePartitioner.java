package com.yy.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yaoyu  2019/3/13 - 16:39
 */


public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        int partition = 4;
        //按照手机号的前三位分区
        String prePhoneNum = text.toString().substring(0, 3);
        if ("136".equals(prePhoneNum)) {
            partition = 0;
        }else if ("137".equals(prePhoneNum)) {
            partition = 1;
        }else if ("138".equals(prePhoneNum)) {
            partition = 2;
        }else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }
        return partition;
    }
}
