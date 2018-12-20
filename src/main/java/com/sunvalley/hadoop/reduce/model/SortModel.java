package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类或方法的功能描述 : 排序的对象序列化
 *
 * @author: logan.zou
 * @date: 2018-12-17 16:45
 */
public class SortModel implements WritableComparable<SortModel> {
    private Text userName;
    private IntWritable money;

    public SortModel() {
    }

    public SortModel(Text userName, IntWritable money) {
        this.userName = userName;
        this.money = money;
    }

    public void set(Text userName, IntWritable money) {
        this.userName = userName;
        this.money = money;
    }

    @Override
    public int compareTo(SortModel sortModel) {
        return sortModel.getMoney().get() - this.money.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userName.write(dataOutput);
        money.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userName = new Text();
        userName.readFields(dataInput);
        money = new IntWritable();
        money.readFields(dataInput);
    }

    public Text getUserName() {
        return userName;
    }

    public void setUserName(Text userName) {
        this.userName = userName;
    }

    public IntWritable getMoney() {
        return money;
    }

    public void setMoney(IntWritable money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return userName.toString() + "\t" + money.get();
    }
}

