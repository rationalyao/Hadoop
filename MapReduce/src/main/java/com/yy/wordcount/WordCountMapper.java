package com.yy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/7 - 14:33
 */

//map阶段
//KEYIN 输入数据的key    VALUEIN 输入数据的value  KEYOUT 输出数据的类型  VALUEOUT 输出数据的值
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        //1.获取一行
        String line = value.toString();
        //2.切割单词
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
