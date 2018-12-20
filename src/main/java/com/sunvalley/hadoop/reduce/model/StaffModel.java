package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类或方法的功能描述 : hadoop 自定义序列化bean
 *
 * @author: logan.zou
 * @date: 2018-12-17 14:08
 */
public class StaffModel implements Writable {
    private Text userName;
    private IntWritable money;
    private Text province;

    /**
     * 反序列化时必须有一个空参的构造方法
     */
    public StaffModel() {
    }

    public StaffModel(Text userName, IntWritable money, Text province) {
        this.userName = userName;
        this.money = money;
        this.province = province;
    }

    /**
     * 反序列化
     * @param output
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        userName.write(output);
        money.write(output);
        province.write(output);
    }

    /**
     * 反序列化代码
     * @param input
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        userName = new Text();
        userName.readFields(input);
        money = new IntWritable();
        money.readFields(input);
        province = new Text();
        province.readFields(input);
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

    public Text getProvince() {
        return province;
    }

    public void setProvince(Text province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return userName.toString() + "," + money.get();
    }
}

