package com.example.covid_merge;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 21:28
 * @Email aiykerwin@sina.com
 */
// Hadoop中自定义的类必须要能够被序列化和反序列化  实现Writable接口
public class CovidCountBean implements Writable {
    /**
     * 累计确诊病例
     */
    private int cases;
    /**
     * deaths累计死亡病例
     */
    private int deaths;

    public CovidCountBean() {
    }

    public CovidCountBean(int cases, int deaths) {
        this.cases = cases;
        this.deaths = deaths;
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
        return cases + "\t" + deaths;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cases);
        out.writeInt(deaths);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cases = in.readInt();
        this.deaths = in.readInt();
    }
}
