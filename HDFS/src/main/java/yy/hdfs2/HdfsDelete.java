package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/4 - 20:48
 */


public class HdfsDelete {
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.删除文件
        fs.delete(new Path("/yaoyu/sanguo.txt"));
        
        // 3.关闭资源
        fs.close();
        
    }
}
