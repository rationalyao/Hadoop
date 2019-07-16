package com.yy.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/16 - 15:48
 */


public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        IntWritable v = new IntWritable(1);

        //1.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        //2.写出
        v.set(sum);
        context.write(key, v);
    }
}
