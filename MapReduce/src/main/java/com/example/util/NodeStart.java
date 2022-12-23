package com.example.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-22 22:39
 * @Email aiykerwin@sina.com
 */
public class NodeStart {

    public void start(Job job, Configuration configuration, String input, String output, String inputNode, String outputNode) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        String os = System.getProperty("os.name");
        System.out.println(os.substring(0, 3));
        boolean node = false;
        if (os.startsWith("Win")) {
        } else {
            node = true;
        }
        if (node) {
            System.out.println("集群运行");
            // 配置job的输入数据路径,一行行读取
            FileInputFormat.addInputPath(job, new Path(inputNode));
            //6:配置job的输出数据路径
            FileOutputFormat.setOutputPath(job, new Path(outputNode));
            // 集群执行
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);
            // 判断输出路径是否存在 如果存在删除    如果目录存在会报错 所以先判断一下如果有此目录先删除
            boolean flag = fileSystem.exists(new Path(outputNode));
            if (flag) {
                fileSystem.delete(new Path(outputNode), true);
            }
            //提交作业并等待执行完成
            boolean bl = job.waitForCompletion(true);
            //程序退出
            System.exit(bl ? 0 : 1);
        } else {
            System.out.println("本地执行");
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            FileSystem fileSystem = FileSystem.get(new URI("file:///"), configuration);
            boolean flag = fileSystem.exists(new Path(output));
            if (flag) {
                fileSystem.delete(new Path(output), true);
            }
            boolean bl = job.waitForCompletion(true);
            System.exit(bl ? 0 : 1);
        }


    }


}
