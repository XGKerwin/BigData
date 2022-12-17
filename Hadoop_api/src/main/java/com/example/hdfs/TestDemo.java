package com.example.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author guanxin
 * @Date 2022-12-10 18:38
 * @Email aiykerwin@sina.com
 */
public class TestDemo {

    private FileSystem fileSystem;

    @Before
    public void init() throws Exception {
        // 获取FileSystem对象
        String url = "hdfs://node1:8020";
        fileSystem = FileSystem.get(new URI(url), new Configuration());
    }

    @After
    public void close() throws Exception {
        // 关闭文件系统
        fileSystem.close();
    }


    @Test
    public void getFileSystem() throws Exception {
        // 获取File对象 测试是否连接成功
        System.out.println(fileSystem);
        // 实现 hadoop fs -ls /功能，列出指定目录下的文件和文件夹
//        listFile();
        // 实现 hadoop fs -ls /功能，列出指定目录下的非文件夹文件
//        listFiles2();
        // 遍历HDFS中所有文件
//        filesList();
        // 创建目录
//        mkdir();
        // 删除目录
//        delete();
        // 从HDFS上下载文件
//        downloadFile();
        // 上传文件
//        put();
//        putFile();
        // 实现小文件合并上传  hadoop fs -appendToFile
        appendToFile();
    }

    private void appendToFile() throws Exception {
        //获取源数据的输入流    此目录下有3个小文件
        File file = new File("E:\\input");
        File[] files = file.listFiles();
        //获取目标文件的输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/guan/big_file.txt"));
        for (File file1 : files) {
            //获取每一个文件的输入流
            FileInputStream fileInputStream = new FileInputStream(file1);
            //将每一个小文件追击到目标文件中    追加复制
            IOUtils.copy(fileInputStream, fsDataOutputStream);
        }
        fsDataOutputStream.close();
    }

    private void put() throws Exception {
        fileSystem.copyFromLocalFile(new Path("D:\\a.txt"), new Path("/guan/b.txt"));
    }


    private void putFile() throws Exception {
        //获取源数据的输入流
        FileInputStream fileInputStream = new FileInputStream("D:\\a.txt");
        //获取目标文件的输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/guanxin/b.txt"));
        //实现文件的复制
        byte[] bytes = new byte[1024];
        while (true) {
            //从源文件读取数据，返回实际读取的字节数
            int len = fileInputStream.read(bytes);
            if (len == -1) {
                break;
            }
            //将读取到的数据写到目标文件
            fsDataOutputStream.write(bytes, 0, len);
        }
        System.out.println(fsDataOutputStream);
        fsDataOutputStream.close();
        fileInputStream.close();
    }

    private void downloadFile() throws Exception {
        fileSystem.copyToLocalFile(new Path("/guan/a.txt"), new Path("D:\\"));
    }

    private void listFile() throws Exception {
        //1:获取/目录下所有文件的详情，并将详情信息存入数组
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/guan"));
        //2：遍历数组，获取每一个文件的具体详情
        for (FileStatus fileStatus : fileStatuses) {
            //获取文件的路径  获取block大小  获取副本数  获取文件的长度
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getLen());
            System.out.println("---------------------------------");
        }
    }

    private void listFiles2() throws IOException {
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/guan"), true);
        //遍历集合
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(fileStatus.getPath());
            //获取文件的Block信息  该数组包含了文件每一个BLock信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //数组的长度就是每一个文件的Block数
            System.out.println(blockLocations.length);
            //遍历每一个Block
            for (BlockLocation blockLocation : blockLocations) {
                //获取Block的3个副本所在的主机信息
                String[] hosts = blockLocation.getHosts();
                System.out.println("副本数：" + hosts.length);
                //获取每一个副本所在的主机
                for (String host : hosts) {
                    System.out.println(host);
                }
                System.out.println("***********");
            }
        }

    }

    private void filesList() throws Exception {
        // 获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().toString());
        }
    }


    private void mkdir() throws Exception {
        boolean mkdirs = fileSystem.mkdirs(new Path("/aiy/yu"));
        System.out.println(mkdirs);
    }


    private void delete() throws Exception {
        boolean delete = fileSystem.delete(new Path("/aiy/yu"));
        System.out.println(delete);
    }


}
