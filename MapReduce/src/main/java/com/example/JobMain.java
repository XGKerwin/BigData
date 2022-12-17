package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-13 23:16
 * @Email aiykerwin@sina.com
 */
public class JobMain {

    /**
     * 创建一个MapReducer作业  并执行
     * 统计个数
     */

    private static final String INPUT = "D:\\bank\\input\\wordcount";
    private static final String OUTPUT = "D:\\bank\\output\\wordcount";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        //配置文件对象
        Configuration configuration = new Configuration();
        //创建作业实例  任务名
        Job job = Job.getInstance(configuration, "wordcount");
        // 设置作业驱动类
        job.setJarByClass(JobMain.class);
        //1:配置job的输入数据路径,一行行读取
        FileInputFormat.addInputPath(job, new Path(INPUT));
        //2：设置job的自定义Mapper类是哪个类
        job.setMapperClass(WordCountMapper.class);
        //3: 设置job的Mapper的K2，V2数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //4:设置job的自定义reducer类是哪个类
        job.setReducerClass(WordCountReducer.class);
        //5:设置job的Reducer的K3，V3数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //6:配置job的输出数据路径,该路径不能存在，否则报错
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

        // 本地执行 & 集群执行   FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);
        FileSystem fileSystem = FileSystem.get(new URI("file:///"), configuration);

        // 判断输出路径是否存在 如果存在删除
        boolean flag = fileSystem.exists(new Path(OUTPUT));
        if (flag) {
            fileSystem.delete(new Path(OUTPUT), true);
        }
        //提交作业并等待执行完成
        boolean bl = job.waitForCompletion(true);
        //程序退出
        System.exit(bl ? 0 : 1);
    }
}
