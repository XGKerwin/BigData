package com.example.covid_partitioner;

import com.example.util.NodeStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-18 18:43
 * @Email aiykerwin@sina.com
 */
public class JobMain {

    private static final String INPUT = "D:\\bank\\input\\covid19";
    private static final String OUTPUT = "D:\\bank\\output\\covid19_partitioner";
    private static final String INPUT_NODE = "hdfs://node1:8020/input/covid19";
    private static final String OUTPUT_NODE = "hdfs://node1:8020/output/covid19_partitioner";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 配置文件对象
        Configuration configuration = new Configuration();
        // 创建作业
        Job job = Job.getInstance(configuration, "covid_partitioner");
        // 设置驱动类
        job.setJarByClass(JobMain.class);
        // 设置自定义Mapper
        job.setMapperClass(CovidBeanMapper.class);
        // 设置job的Mapper的K2，V2数据类型
        job.setMapOutputKeyClass(CovidBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置job的自定义reducer类是哪个类
        job.setReducerClass(CovidBeanReducer.class);
        job.setOutputKeyClass(CovidBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置你自定义的分区类是谁
        job.setPartitionerClass(CovidPartitioner.class);
        // 设置Reduce的个数
        job.setNumReduceTasks(55);

        new NodeStart().start(job, configuration, INPUT, OUTPUT, INPUT_NODE, OUTPUT_NODE);

    }

}
