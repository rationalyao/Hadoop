package com.yy.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/8 - 14:41
 */


public class FlowsumDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/aaa/serialize", "D:/aaa/outputfenqu"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);
        //2.设置jar的路径
        job.setJarByClass(FlowsumDriver.class);
        //3.关联mapper和reduce
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //4.设置mapper输出的key和value类型
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置分区关联
        /*job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);*/
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }
}
