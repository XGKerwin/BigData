package com.example.wordcount_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author guanxin
 * @Date 2022-12-18 12:55
 * @Email aiykerwin@sina.com
 */
public class WordCountPartitioner2 extends Partitioner<Text, LongWritable> {
    /**
     * 如果单词长度 <5,结果存入 0
     * 如果单词长度 =5,结果存入 1
     * 如果单词长度 >5,结果存入 2
     *
     * @param text         key
     * @param longWritable value
     * @param i            reduce 个数
     * @return 打的标记
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        String key = text.toString();
        if (key.length() < 5) {
            return 0;
        } else if (key.length() == 5) {
            return 1;
        } else {
            return 2;
        }
    }
}
