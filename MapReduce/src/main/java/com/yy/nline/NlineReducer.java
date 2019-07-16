package com.yy.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/12 - 13:55
 */


public class NlineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.累加求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //2.写出
        v.set(sum);
        context.write(key, v);
    }
}
