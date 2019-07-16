package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
 * @author yaoyu  2019/7/5 - 13:35
 */


public class HdfsFromHDFS {
    
    @Test
    public void testFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/yaoyu/animals.txt"));
        
        // 3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/a.txt"));

        // 4.流的对拷
        IOUtils.copyBytes(fis, fos, conf);
        
        // 5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
