package com.example.covid_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-18 18:26
 * @Email aiykerwin@sina.com
 */
public class CovidBeanMapper extends Mapper<LongWritable, Text, CovidBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        //0：如果读取的行数据不全，则进行预处理(抛弃)
        if (split.length < 6) {
            return;
        }
        //1:得到K2--->拆分V1，定义CovidBean类对象，封装对象

        CovidBean covidBean = new CovidBean();
        covidBean.setDate(split[0]);
        covidBean.setCounty(split[1]);
        covidBean.setState(split[2]);
        covidBean.setFips(split[3]);
        covidBean.setCases(Integer.parseInt(split[4]));
        covidBean.setDeaths(Integer.parseInt(split[5]));

        //2:得到V2--->NullWritable
        //3:将K2和V2写入上下文
        context.write(covidBean, NullWritable.get());

    }
}
