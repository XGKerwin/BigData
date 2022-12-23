package com.example.covid_grouping;

import com.example.util.NodeStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:34
 * @Email aiykerwin@sina.com
 */
public class JobMain {

    private static final String INPUT = "D:\\bank\\input\\covid19";
    private static final String OUTPUT = "D:\\bank\\output\\covid19_grouping";
    private static final String INPUT_NODE = "hdfs://node1:8020/input/covid19";
    private static final String OUTPUT_NODE = "hdfs://node1:8020/output/covid19_grouping";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.maxsize","67108864");
        Job job = Job.getInstance(configuration, "covid_grouping");

        job.setJarByClass(JobMain.class);

        job.setMapperClass(CovidGroupingMapper.class);

        job.setMapOutputKeyClass(CovidGroupingBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CovidGroupingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(MyGrouping.class);

        NodeStart nodeStart = new NodeStart();
        nodeStart.start(job,configuration,INPUT,OUTPUT,INPUT_NODE,OUTPUT_NODE);
    }
}
