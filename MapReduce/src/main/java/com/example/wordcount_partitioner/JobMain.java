package com.example.wordcount_partitioner;

import com.example.util.NodeStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-18 14:10
 * @Email aiykerwin@sina.com
 */
public class JobMain {

    private static final String INPUT = "D:\\bank\\input\\wordcount";
    private static final String OUTPUT = "D:\\bank\\output\\wordcount_partitioner";
    private static final String INPUT_NODE = "hdfs://node1:8020/input/wordcount";
    private static final String OUTPUT_NODE = "hdfs://node1:8020/output/wordcount_partitioner";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        // 配置文件对象
        Configuration configuration = new Configuration();
        // 创建作业实例
        Job job = Job.getInstance(configuration, "covid19");
        // 设置作业驱动类
        job.setJarByClass(JobMain.class);
        //  设置job的自定义Mapper类是哪个类
        job.setMapperClass(WordCountMapper.class);

        //  设置job的Mapper的K2，V2数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置job的自定义reducer类是哪个类
        job.setReducerClass(WordCountReducer.class);

        // 设置job的Reducer的K3，V3数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置你自定义的分区类
        job.setPartitionerClass(WordCountPartitioner2.class);
        // 设置Reduce的个数（默认是一个） 分区个数
        job.setNumReduceTasks(3);

        new NodeStart().start(job, configuration, INPUT, OUTPUT, INPUT_NODE, OUTPUT_NODE);

    }

}
