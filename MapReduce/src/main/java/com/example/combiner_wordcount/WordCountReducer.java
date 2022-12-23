package com.example.combiner_wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:17
 * @Email aiykerwin@sina.com
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));

    }
}
