package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类或方法的功能描述 : 分组排序model类
 *
 * @author: logan.zou
 * @date: 2019-01-11 13:38
 */
public class GroupSortModel implements WritableComparable<GroupSortModel> {
    private int name;
    private int num;

    public GroupSortModel() {
    }

    public GroupSortModel(int name, int num) {
        this.name = name;
        this.num = num;
    }

    public void set(int name, int num) {
        this.name = name;
        this.num = num;
    }

    @Override
    public int compareTo(GroupSortModel groupSortModel) {
        if (this.name != groupSortModel.getName()) {
            return this.name < groupSortModel.getName() ? -1 : 1;
        } else if (this.num != groupSortModel.getNum()) {
            return this.num < groupSortModel.getNum() ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(this.name);
        output.writeInt(this.num);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.name = input.readInt();
        this.num = input.readInt();
    }

    @Override
    public String toString() {
        return name + "\t" + num;
    }

    @Override
    public int hashCode(){
        return this.name * 157 + this.num;
    }

    public int getName() {
        return name;
    }

    public void setName(int name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

