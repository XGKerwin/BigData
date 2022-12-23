package com.example.covid_grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guanxin
 * @Date 2022-12-22 23:48
 * @Email aiykerwin@sina.com
 */
public class CovidGroupingBean implements WritableComparable<CovidGroupingBean> {

    private String state;
    private int cases;

    public CovidGroupingBean() {
    }

    public CovidGroupingBean(String state, int cases) {
        this.state = state;
        this.cases = cases;
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

    @Override
    public String toString() {
        return "CovidGroupingBean{" + "state='" + state + '\'' + ", cases=" + cases + '}';
    }

    @Override
    public int compareTo(CovidGroupingBean o) {
        int result = this.getState().compareTo(o.getState());
        if (result == 0) {
            return (this.getCases() - o.getCases()) * -1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(state);
        out.writeInt(cases);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.state = in.readUTF();
        this.cases = in.readInt();
    }
}
