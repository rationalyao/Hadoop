package com.yy.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.security.Key;

/**
 * @author yaoyu  2019/3/14 - 14:24
 */


public class FRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream fosbaidu;
    FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {
        //1.获取文件系统
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //2.创建输出到baidu.log的输出流
            fosbaidu = fs.create(new Path("D:/aaa/baidu.log"));
            //3.创建输出到other.log的输出流
            fosother = fs.create(new Path("D:/aaa/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断key当中是否有baidu，如果有写出到baidu.log，如果没有，写出到other.log
        if (key.toString().contains("baidu")) {
            //baidu输出流
            fosbaidu.write(key.toString().getBytes());
        }else {
            fosother.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fosbaidu);
        IOUtils.closeStream(fosother);
    }
}
