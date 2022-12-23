package com.example.covid_sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 22:24
 * @Email aiykerwin@sina.com
 */
public class CovidSortBean implements WritableComparable<CovidSortBean> {
    /**
     * 洲
     */
    private String state;
    /**
     * 累计确诊病例
     */
    private int cases;
    /**
     * 累计死亡病例
     */
    private int deaths;

    public CovidSortBean() {
    }

    public CovidSortBean(String state, int cases, int deaths) {
        this.state = state;
        this.cases = cases;
        this.deaths = deaths;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getCases() {
        return cases;
    }

    public void setCases(int cases) {
        this.cases = cases;
    }

    public int getDeaths() {
        return deaths;
    }

    public void setDeaths(int deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return state + "\t" + cases + "\t" + deaths;
    }

    /**
     * 1:在MR中对K2的排序不用我们去排序，我们只需要指定排序规则，MR会自动按照我们设置的规则进行排序
     * 1) 我们按照确诊病例进行降序排序
     * 2) compareTo只需要告诉MR什么时候返回大于0的值，什么时候返回小于0的值，什么时候返回等于0的值
     * 2:MR内部使用的是：归并和快排
     *
     * @param o the object to be compared.
     * @return
     */
    @Override
    public int compareTo(CovidSortBean o) {
        int result = this.getCases() - o.getCases();
        return result * -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(state);
        out.writeInt(cases);
        out.writeInt(deaths);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.state = in.readUTF();
        this.cases = in.readInt();
        this.deaths = in.readInt();
    }
}
