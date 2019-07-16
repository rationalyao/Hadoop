package yy.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author yaoyu  2019/7/5 - 9:16
 */


public class HdfsListFiles {
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master01:9000"), conf, "root");
        
        // 2.获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------------------------");
        }
        fs.close();
    }
}























