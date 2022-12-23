package com.example.covid_grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author guanxin
 * @Date 2022-12-23 0:04
 * @Email aiykerwin@sina.com
 */
public class MyGrouping extends WritableComparator {

    public MyGrouping() {
        // 允许父类通过反射来创建JavaBaen的对象
        super(CovidGroupingBean.class, true);
    }

    /**
     * 自定义分组规则
     *  只要两个CovidGroupingBean的州（state）相同,compare就返回0，则就应该在一组
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CovidGroupingBean c1 = (CovidGroupingBean) a;
        CovidGroupingBean c2 = (CovidGroupingBean) b;
        return c1.getState().compareTo(c2.getState());
    }
}