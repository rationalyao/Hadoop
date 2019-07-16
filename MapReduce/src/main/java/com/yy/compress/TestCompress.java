package com.yy.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @author yaoyu  2019/3/16 - 11:39
 */


public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //压缩
        //compress("D:/aaa/compress/baidu.txt", "org.apache.hadoop.io.compress.BZip2Codec");

        decompress("D:/aaa/compress/baidu.txt.bz2");
    }

    private static void decompress(String fileName) throws IOException {
        //1.压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not process");
            return;
        }

        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);
        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));
        //4.流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);
        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    @SuppressWarnings({"resource", "unchecked"})
    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        Class classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());
        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //3.流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);
        //4.关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
