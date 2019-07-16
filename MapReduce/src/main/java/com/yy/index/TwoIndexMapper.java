package com.yy.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/16 - 16:06
 */


public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切割
        String[] fields = line.split("--");
        //3.写出
        k.set(fields[0]);
        v.set(fields[1]);
        //4.写出
        context.write(k, v);

    }
}
