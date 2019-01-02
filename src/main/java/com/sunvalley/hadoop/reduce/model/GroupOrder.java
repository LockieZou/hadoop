package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类或方法的功能描述 : 使用GroupingComparator分组计算最大值
 *
 * @author: logan.zou
 * @date: 2018-12-29 17:02
 */
public class GroupOrder implements WritableComparable<GroupOrder> {
    private Text itemid;
    private DoubleWritable amount;

    public GroupOrder() {
    }

    public GroupOrder(Text itemid, DoubleWritable amount) {
        set(itemid, amount);
    }

    public void set(Text itemid, DoubleWritable amount) {
        this.itemid = itemid;
        this.amount = amount;
    }

    @Override
    public int compareTo(GroupOrder o) {
        int itemidTmp = this.itemid.compareTo(o.getItemid());
        int amountTmp = this.amount.compareTo(o.getAmount());
        if (itemidTmp == 0) {
            itemidTmp = -amountTmp;
        }
        return itemidTmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(itemid.toString());
        dataOutput.writeDouble(amount.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String readUTF = dataInput.readUTF();
        Double readDouble = dataInput.readDouble();

        this.itemid = new Text(readUTF);
        this.amount = new DoubleWritable(readDouble);
    }

    public Text getItemid() {
        return itemid;
    }

    public void setItemid(Text itemid) {
        this.itemid = itemid;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return itemid.toString() + "\t" + amount.get();
    }
}

