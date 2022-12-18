package com.example.wordcount_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author guanxin
 * @Date 2022-12-18 14:02
 * @Email aiykerwin@sina.com
 */
public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {

        String k2 = text.toString();
        // 0 1  数据倾斜 打散数据
        int num = Math.abs(k2.hashCode()) % i;
        return num;
    }
}
