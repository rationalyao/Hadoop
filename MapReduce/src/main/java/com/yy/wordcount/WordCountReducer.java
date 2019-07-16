package com.yy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author yaoyu  2019/3/7 - 14:49
 */

//KEYIN, VALUEIN map阶段输出的key和value
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.累加求和
        int sum = 0;
        for (IntWritable value : values) {
            //sum类型为int，value类型为intwritable，所以需要get方法
            sum += value.get();
        }
        //2.写出
        IntWritable v = new IntWritable();
        v.set(sum);
        context.write(key, v);
    }
}
