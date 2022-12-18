package com.example.covid_partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-18 18:34
 * @Email aiykerwin@sina.com
 */
public class CovidBeanReducer extends Reducer<CovidBean, NullWritable, CovidBean, NullWritable> {

    @Override
    protected void reduce(CovidBean key, Iterable<NullWritable> values, Reducer<CovidBean, NullWritable, CovidBean, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
