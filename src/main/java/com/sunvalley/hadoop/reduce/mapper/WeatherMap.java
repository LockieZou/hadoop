package com.sunvalley.hadoop.reduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 类或方法的功能描述 : 读取一年中某天的最高气温
 *
 * @author: logan.zou
 * @date: 2018-12-07 11:43
 */
public class WeatherMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>  {
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        // 打印输入样本 如 2018120715
        System.out.println("==== Before Mapper: ====" + key + "," +value);
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
//        while (tokenizer.hasMoreTokens()) {
            // 截取年份
            String year = line.substring(0, 4);
            // 截取温度
            int temperature = Integer.parseInt(line.substring(8));
            word.set(year);
            output.collect(word, new LongWritable(temperature));

            // 打印输出样本
            System.out.println("==== After Mapper: ==== " + new Text(year) + "," + new IntWritable(temperature));
//        }
    }
}

