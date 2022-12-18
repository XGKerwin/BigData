package com.example.wordcount_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-18 14:04
 * @Email aiykerwin@sina.com
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //2:重写Map方法，该方法是自动被调用，有多少行数据，该方法就执行多少次

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //3:在该方法中将K1和V1转成K2和V2
        //3.1 得到K2 --->对V1进行拆分，得到的每一个单词就是K2
        String[] words = value.toString().split(",");

        //3.2 得到V2--->V2就是固定值1，所以不用得到
        //3.3 将K2和V2写入下一个处理流程(上下文中)
        for (String k2 : words) {
            //context表示上下文对象
            context.write(new Text(k2), new LongWritable(1));
        }

    }
}
