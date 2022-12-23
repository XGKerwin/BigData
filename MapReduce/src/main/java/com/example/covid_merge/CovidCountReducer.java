package com.example.covid_merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 21:45
 * @Email aiykerwin@sina.com
 */
public class CovidCountReducer extends Reducer<Text, CovidCountBean, Text, CovidCountBean> {

    @Override
    protected void reduce(Text key, Iterable<CovidCountBean> values, Reducer<Text, CovidCountBean, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        int cases = 0;
        int deaths = 0;
        for (CovidCountBean value : values) {
            cases += value.getCases();
            deaths += value.getDeaths();
        }
        CovidCountBean covidCountBean = new CovidCountBean(cases, deaths);
        context.write(key, covidCountBean);
    }

}
