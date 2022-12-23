package com.example.covid_grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-23 0:01
 * @Email aiykerwin@sina.com
 */
public class CovidGroupingReducer extends Reducer<CovidGroupingBean, Text, Text, NullWritable> {

    @Override
    protected void reduce(CovidGroupingBean key, Iterable<Text> values, Reducer<CovidGroupingBean, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text value : values) {
            context.write(value, NullWritable.get());
            if (++i >= 3) {
                break;
            }
        }
    }
}
