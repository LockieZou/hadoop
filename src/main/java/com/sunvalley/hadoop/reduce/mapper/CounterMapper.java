package com.sunvalley.hadoop.reduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 全局统计计数器
 *
 * @author: logan.zou
 * @date: 2019-01-02 14:05
 */
public class CounterMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    // 通过枚举形式自定义计数器
    enum MyCounter {MALFORORMED,NORMAL}

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }

        // 对枚举定义的计数器加1
        context.getCounter(MyCounter.MALFORORMED).increment(1);
        // 通过动态设置自定义计数器加1
        context.getCounter("counterGroupa", "countera").increment(1);
        // 设置数值
        context.getCounter("","").setValue(10);
    }
}

