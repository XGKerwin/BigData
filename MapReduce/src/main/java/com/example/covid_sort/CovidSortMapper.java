package com.example.covid_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 22:21
 * @Email aiykerwin@sina.com
 */
public class CovidSortMapper extends Mapper<LongWritable, Text, CovidSortBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidSortBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        CovidSortBean covidSortBean = new CovidSortBean(split[0],Integer.parseInt(split[1]),Integer.parseInt(split[2]));
        context.write(covidSortBean,NullWritable.get());
    }

}
