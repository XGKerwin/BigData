package com.example.covid_grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:47
 * @Email aiykerwin@sina.com
 */
public class CovidGroupingMapper extends Mapper<LongWritable, Text, CovidGroupingBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidGroupingBean, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if (split.length < 6) {
            return;
        }
        CovidGroupingBean covidGroupingBean = new CovidGroupingBean(split[2], Integer.parseInt(split[4]));
        context.write(covidGroupingBean, value);
    }
}
