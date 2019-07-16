package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/5 - 9:33
 */


public class HdfsToHDFS {
    @Test
    public void testToHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.创建输入流
        FileInputStream fis  = new FileInputStream(new File("d:/Hadoop/animal.txt"));
        
        // 3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/yaoyu/animals.txt"));
        
        // 4.流的对拷
        IOUtils.copyBytes(fis, fos, conf);
        
        // 5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
