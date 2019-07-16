package yy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/3/6 - 14:19
 */


public class HDFSIO {
    //把本地D盘的yuexiaduzhuo。txt文件上传到HDFS目录
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File("D:/yuexiaduzhuo.txt"));
        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/libai.txt"));
        //4.流的对拷
        IOUtils.copyBytes(fis,fos,conf);
        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //2.下载文件到本地
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.获取资源
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/libai.txt"));
        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/aaa/yxdz.txt"));
        //4.流的对拷
        IOUtils.copyBytes(fis,fos,conf);
        //5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    //3.1 下载第一块
    @Test
    public void readFileSeek1() throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/aaa/hadoop-2.7.2.tar.gz"));
        //4.流的对拷(只拷128M)
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //3.2 下载第二块
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3.获取指定的读取节点
        fis.seek(1024*1024*128);
        //4.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/aaa/2.hadoop-2.7.2.tar.gz"));
        //5.流的对拷（）
        IOUtils.copyBytes(fis, fos, conf);         //没有这步，文件下载后大小为0
        //6.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
