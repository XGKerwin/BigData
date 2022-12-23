package com.example.combiner_wordcount;

import com.example.util.NodeStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:07
 * @Email aiykerwin@sina.com
 */
public class JobMain {

    private static final String INPUT = "D:\\bank\\input\\covid19";
    private static final String OUTPUT = "D:\\bank\\output\\covid19_combiner";
    private static final String INPUT_NODE = "hdfs://node1:8020/input/covid19";
    private static final String OUTPUT_NODE = "hdfs://node1:8020/output/covid19_combiner";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "combiner_wordcount");
        job.setJarByClass(JobMain.class);

        job.setMapperClass(WordCountMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setCombinerClass(WordCountReducer.class);

        NodeStart nodeStart = new NodeStart();
        nodeStart.start(job, configuration, INPUT, OUTPUT, INPUT_NODE, OUTPUT_NODE);

    }

}
