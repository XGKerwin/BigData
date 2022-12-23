package com.example.covid_sort;

import com.example.util.NodeStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-22 22:18
 * @Email aiykerwin@sina.com
 */
public class JobMain {
    private static final String INPUT = "D:\\bank\\input\\covid19_sort";
    private static final String OUTPUT = "D:\\bank\\output\\covid19_sort";
    private static final String INPUT_NODE = "hdfs://node1:8020/input/covid19_sort";
    private static final String OUTPUT_NODE = "hdfs://node1:8020/output/covid_sort";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "covid_sort");
        job.setJarByClass(JobMain.class);

        job.setMapperClass(CovidSortMapper.class);

        job.setMapOutputKeyClass(CovidSortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(CovidSortReducer.class);

        job.setOutputKeyClass(CovidSortBean.class);
        job.setOutputValueClass(NullWritable.class);
        NodeStart nodeStart = new NodeStart();
        nodeStart.start(job,configuration,INPUT,OUTPUT,INPUT_NODE,OUTPUT_NODE);

    }
}
