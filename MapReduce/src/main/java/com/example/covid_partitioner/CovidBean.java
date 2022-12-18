package com.example.covid_partitioner;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-18 18:17
 * @Email aiykerwin@sina.com
 */
public class CovidBean implements WritableComparable {
    private String date; //日期
    private String county; //县
    private String state;// 州
    private String fips;// 县编码code
    private int cases; //累计确诊病例
    private int deaths; // deaths累计死亡病例

    public CovidBean() {
    }

    public CovidBean(String date, String county, String state, String fips, int cases, int deaths) {
        this.date = date;
        this.county = county;
        this.state = state;
        this.fips = fips;
        this.cases = cases;
        this.deaths = deaths;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public String getFips() {
        return fips;
    }

    public void setFips(String fips) {
        this.fips = fips;
    }

    //实现序列化,当对象要被序列化时，则会自动调用该方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeUTF(county);
        out.writeUTF(state);
        out.writeUTF(fips);
        out.writeInt(cases);
        out.writeInt(deaths);
    }

    //实现反序列化,当对象要被反序列化时，则自动调用该方法，而且序列化的顺序一定和反序列化顺序一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.county = in.readUTF();
        this.state = in.readUTF();
        this.fips = in.readUTF();
        this.cases = in.readInt();
        this.deaths = in.readInt();
    }

    @Override
    public int compareTo(Object o) {
        return 1;
    }

    @Override
    public String toString() {
        return date + "|+|" + county + "|+|" + state + "|+|" + fips + "|+|" + cases + "|+|" + deaths;
    }
}
