package com.example.covid_partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author guanxin
 * @Date 2022-12-18 18:35
 * @Email aiykerwin@sina.com
 */
public class CovidPartitioner extends Partitioner<CovidBean, NullWritable> {
    static HashMap<String, Integer> stateMap = new HashMap<>();
    // 精准分区
    
//    static {
//        stateMap.put("Alabama", 0);
//        stateMap.put("Alaska", 1);
//        stateMap.put("Arizona", 2);
//        stateMap.put("Arkansas", 3);
//        stateMap.put("California", 4);
//        stateMap.put("Colorado", 5);
//        stateMap.put("Connecticut", 6);
//        stateMap.put("Delaware", 7);
//        stateMap.put("District of Columbia", 8);
//        stateMap.put("Florida", 9);
//        stateMap.put("Georgia", 10);
//        stateMap.put("Guam", 11);
//        stateMap.put("Hawaii", 12);
//        stateMap.put("Idaho", 13);
//        stateMap.put("Illinois", 14);
//        stateMap.put("Indiana", 15);
//        stateMap.put("Iowa", 16);
//        stateMap.put("Kansas", 17);
//        stateMap.put("Kentucky", 18);
//        stateMap.put("Louisiana", 19);
//        stateMap.put("Maine", 20);
//        stateMap.put("Maryland", 21);
//        stateMap.put("Massachusetts", 22);
//        stateMap.put("Michigan", 23);
//        stateMap.put("Minnesota", 24);
//        stateMap.put("Mississippi", 25);
//        stateMap.put("Missouri", 26);
//        stateMap.put("Montana", 27);
//        stateMap.put("Nebraska", 28);
//        stateMap.put("Nevada", 29);
//        stateMap.put("New Hampshire", 30);
//        stateMap.put("New Jersey", 31);
//        stateMap.put("New Mexico", 32);
//        stateMap.put("New York", 33);
//        stateMap.put("North Carolina", 34);
//        stateMap.put("North Dakota", 35);
//        stateMap.put("Northern Mariana Islands", 36);
//        stateMap.put("Ohio", 37);
//        stateMap.put("Oklahoma", 38);
//        stateMap.put("Oregon", 39);
//        stateMap.put("Pennsylvania", 40);
//        stateMap.put("Puerto Rico", 41);
//        stateMap.put("Rhode Island", 42);
//        stateMap.put("South Carolina", 43);
//        stateMap.put("South Dakota", 44);
//        stateMap.put("Tennessee", 45);
//        stateMap.put("Texas", 46);
//        stateMap.put("Utah", 47);
//        stateMap.put("Vermont", 48);
//        stateMap.put("Virgin Islands", 49);
//        stateMap.put("Virginia", 50);
//        stateMap.put("Washington", 51);
//        stateMap.put("West Virginia", 52);
//        stateMap.put("Wisconsin", 53);
//        stateMap.put("Wyoming", 54);
//    }

    private int z = -1;

    /**
     * 根据CovidBean类中的州来进行分区
     *
     * @param covidBean    the key to be partioned.
     * @param nullWritable the entry value.
     * @param i            the total number of partitions.
     * @return
     */
    @Override
    public int getPartition(CovidBean covidBean, NullWritable nullWritable, int i) {
        String state = covidBean.getState();
        System.out.println("--------------" + i);
        if (z == -1) {
            z = 0;
            Integer put = stateMap.put(state, z);
        } else {
            Integer put = stateMap.put(state, z);
            if (put == null) {
                z++;
            }
        }

        return stateMap.get(state);
    }
}
