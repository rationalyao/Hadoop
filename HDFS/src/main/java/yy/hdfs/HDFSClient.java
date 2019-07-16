package yy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import sun.nio.cs.ext.PCK;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author yaoyu  2019/3/5 - 16:03
 */


public class HDFSClient {
    public static void main(String[] args) throws Exception{
        //demo1();
        //demo2();
    }

    private static void demo2() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        fs.mkdirs(new Path("/yy/first/banzhang"));
    }

    private static void demo1() throws IOException {
        //1.获取HDFS客户端对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop101:9000");

        FileSystem fs = FileSystem.get(conf);
        //2.在HDFS上创建路径
        fs.mkdirs(new Path("/yy/first"));
        //3.关闭资源
        fs.close();
        System.out.println("over");
    }

    //1.文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException,URISyntaxException{
        //1.获取fs对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),conf,"root");
        //2.执行上传代码
        fs.copyFromLocalFile(new Path("D:/xxx.txt"),new Path("/yyx.txt"));
        //3.关闭资源
        fs.close();
    }

    //2.文件下载
    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),conf,"root");
        //2.执行下载操作
        //fs.copyToLocalFile(new Path("/yyx.txt"),new Path("D:/download.txt"));
        fs.copyToLocalFile(false, new Path("/yyx.txt"), new Path("D:/xiaohua.txt"),true);
        //3.关闭资源
        fs.close();

    }

    //3.文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.删除文件
        fs.delete(new Path("/yyx.txt"), false);
        fs.delete(new Path("/yy"), true);
        //3.关闭文件
        fs.close();
    }

    //4.文件改名
    @Test
    public void testRename() throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),conf,"root");
        //2.执行更名操作
        fs.rename(new Path("/yyy.txt"), new Path("/aaa.txt"));
        //3.关闭文件
        fs.close();
    }

    //5.文件详情查看
    @Test
    public void testListFiles() throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            //ctrl + alt + v 快速创建对应对象
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());    //文件名称
            System.out.println(fileStatus.getPermission());        //文件权限
            System.out.println(fileStatus.getLen());               //文件长度
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("------------------------------------------");
        }
        //3.关闭文件
        fs.close();
    }

    //6.判断是文件还是文件夹
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
        //2.判断操作
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            }else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        //3.关闭文件
        fs.close();
    }
}
