package com.sunvalley.hadoop.reduce.mapper;

import com.sunvalley.hadoop.reduce.model.GroupOrder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 分组统计mapper
 *
 * @author: logan.zou
 * @date: 2018-12-29 17:50
 */
public class GroupOrderMapper extends Mapper<LongWritable, Text, GroupOrder, NullWritable> {
    GroupOrder bean = new GroupOrder();

    /**
     * 读取 groupOrder.txt 内容格式 1号订单,200
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
        String[] fields = line.split(",");

        // 订单号
        String itemid = fields[0];
        // 价格
        String amount = fields[1];
        bean.set(new Text(itemid), new DoubleWritable(Double.valueOf(amount)));
        context.write(bean, NullWritable.get());
    }
}

