package com.example.covid_sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 22:34
 * @Email aiykerwin@sina.com
 */
public class CovidSortReducer extends Reducer<CovidSortBean, NullWritable, CovidSortBean, NullWritable> {

    @Override
    protected void reduce(CovidSortBean key, Iterable<NullWritable> values, Reducer<CovidSortBean, NullWritable, CovidSortBean, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
