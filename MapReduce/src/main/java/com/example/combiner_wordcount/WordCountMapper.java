package com.example.combiner_wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:10
 * @Email aiykerwin@sina.com
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        for (String s : split) {
            context.write(new Text(s), new LongWritable(1));
        }
    }

}
