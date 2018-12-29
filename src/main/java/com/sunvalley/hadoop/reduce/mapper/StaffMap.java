package com.sunvalley.hadoop.reduce.mapper;

import com.sunvalley.hadoop.reduce.model.StaffModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 员工统计mapper类，统计员工的报销总数
 *
 * @author: logan.zou
 * @date: 2018-12-17 14:17
 */
public class StaffMap extends Mapper<LongWritable, Text, Text, StaffModel> {

    /**
     * 读取staff.txt 内容格式： 张三    江西    打车    200
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 防止中文乱码
        String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
        // 根据tab分隔，四个空格
        String[] arr = line.split("    ");
        String userName = arr[0];
        String province = arr[1];
        String type = arr[2];
        int money = Integer.parseInt(arr[3]);

        StaffModel staffModel = new StaffModel();
        staffModel.setUserName(new Text(userName));
        staffModel.setProvince(new Text(province));
        staffModel.setMoney(new IntWritable(money));
        context.write(new Text(userName), staffModel);
    }
}

