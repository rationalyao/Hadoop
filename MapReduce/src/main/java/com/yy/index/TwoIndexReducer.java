package com.yy.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/16 - 16:10
 */


public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        Text v = new Text();

        //1.累加
        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");
        }
        v.set(sb.toString());
        //2.写出
        context.write(key, v);
    }
}
