package com.yy.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yaoyu  2019/3/14 - 15:45
 */


public class FileterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] {"D:/aaa/inputoutputformat", "D:/aaa/outputformat"};
        //1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.加载jar包路径
        job.setJarByClass(FileterDriver.class);
        //3.管理map和reduce
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        //4.设置map输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FilterOutputFormat.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
