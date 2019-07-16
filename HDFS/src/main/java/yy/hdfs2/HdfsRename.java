package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/5 - 8:31
 */


public class HdfsRename {
    @Test
    public void testRename() throws IOException, URISyntaxException, InterruptedException {
        
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.修改文件名称
        fs.rename(new Path("/yaoyu/caocao.txt"), new Path("/yaoyu/cc.txt"));
        
        // 3.关闭资源
        fs.close();
    }
}
