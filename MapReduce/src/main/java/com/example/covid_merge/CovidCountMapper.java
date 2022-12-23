package com.example.covid_merge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 21:27
 * @Email aiykerwin@sina.com
 */
public class CovidCountMapper extends Mapper<LongWritable, Text, Text, CovidCountBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if (split.length < 6) {
            return;
        }
        // 洲当作 key
        String k2 = split[2];
        CovidCountBean covidCountBean = new CovidCountBean(Integer.parseInt(split[4]), Integer.parseInt(split[5]));
        context.write(new Text(k2), covidCountBean);
    }
}
