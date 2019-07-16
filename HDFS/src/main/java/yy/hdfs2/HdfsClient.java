package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/4 - 20:36
 */


public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        
        // 1.获取文件系统
        Configuration conf = new Configuration();
        
        // 2.创建目录
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 3.创建目录
        fs.mkdirs(new Path("/yy/hh"));
        
        // 4.关闭资源
        fs.close();
                

    }
}
