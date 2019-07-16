package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yaoyu  2019/7/5 - 9:26
 */


public class HdfsJudge {
    @Test
    public void testJudge() throws IOException, URISyntaxException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.判断
        FileStatus[] listStatus = fs.listStatus(new Path("/yaoyu/cc.txt"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("f:" + status.getPath().getName());
            }else {
                System.out.println("d:" + status.getPath().getName());
            }
        }
        // 3.关闭资源
        fs.close();
    }
}
