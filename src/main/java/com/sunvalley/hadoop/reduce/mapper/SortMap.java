package com.sunvalley.hadoop.reduce.mapper;

import com.sunvalley.hadoop.reduce.model.SortModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 带排序的对象map类，对员工工资排序，默认为倒序
 *
 * @author: logan.zou
 * @date: 2018-12-17 16:54
 */
public class SortMap extends Mapper<LongWritable,Text,SortModel,Text> {
    private SortModel sortModel = new SortModel();
    private IntWritable moneyWritable = new IntWritable();
    private Text text = new Text();

    /**
     * 文件内容格式: 张三    2980
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 防止中文乱码
        String line = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        // 根据tab分隔，四个空格
        String[] split = line.split("    ");

        // 读取文件格式长度是2个的
        if (split.length == 2) {
            String name = split[0];
            int money = Integer.parseInt(split[1]);
            text.set(name);
            moneyWritable.set(money);
            sortModel.set(text, moneyWritable);
            context.write(sortModel,text);
        }
    }
}

