package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/5 - 14:16
 */


public class HdfsReadBig {
    
    @Test
    public void testReadFileSeek1() throws IOException, URISyntaxException, InterruptedException {
        
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        
        // 3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/hadoop/hadoop-part1"));
        
        // 4.流的对拷
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        
        // 5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
    
    @Test
    public void testReadFileSeek2() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hadoop-master01:9000"), conf, "root");
        
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        fis.seek(1024 * 1024 * 18); 
        
        FileOutputStream fos = new FileOutputStream(new File("d:/hadoop-part2"));
        
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
