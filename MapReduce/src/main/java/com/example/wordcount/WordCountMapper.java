package com.example.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-13 23:15
 * @Email aiykerwin@sina.com
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 得到 k2 对 v1 进行拆分，得到的每一个单词就是 k2
        String[] words = value.toString().split(",");

        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }

    }

}