package com.sunvalley.hadoop.reduce.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * 类或方法的功能描述 : 统计一年天气最高温
 *
 * @author: logan.zou
 * @date: 2018-12-07 13:49
 */
public class WeatherReduce  extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        StringBuffer sb = new StringBuffer();

        // 取values温度的最大值
        while (values.hasNext()) {
            int tmp = values.next().get();
            maxValue = Math.max(maxValue, tmp);
            sb.append(tmp).append(", ");

            output.collect(key, new IntWritable(maxValue));
        }
        // 打印输入样本，如 2000， 15 ，99， 12
        System.out.println("==== Before Reduce ==== " + key + ", " + sb.toString());
        // 打印输出样本
        System.out.println("==== After Reduce ==== " + key + ", " + sb.toString());

    }
}

