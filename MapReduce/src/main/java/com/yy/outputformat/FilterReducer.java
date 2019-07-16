package com.yy.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/14 - 14:17
 */


public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text key = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();

        line = line + "\r\n";

        key.set(line);

        //防止有重复的数据
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
