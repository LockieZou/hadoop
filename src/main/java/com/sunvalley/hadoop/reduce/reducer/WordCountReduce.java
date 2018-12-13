package com.sunvalley.hadoop.reduce.reducer;

import com.sunvalley.hadoop.util.JsonUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

/**
 * 类或方法的功能描述 :TODO
 *
 * @author: logan.zou
 * @date: 2018-12-05 18:29
 */
public class WordCountReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    List<String> textList = null;

    public WordCountReduce() {
        textList = new ArrayList<>();
        textList.add("lisi");
        textList.add("xiaoming");
    }

    /**
     * 执行统计分词
     * @param key
     * @param values
     * @param output
     * @param reporter
     * @throws IOException
     */
    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        int sum = 0;
        System.out.println("*********");
//        System.out.println(key);
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new LongWritable(sum));
        String keyStr = new String(key.toString());
        boolean isHas = textList.contains(keyStr);
        if (isHas) {
            System.out.println("============ " + keyStr + " 统计分词为: " + sum + " ============");
        }
    }
} 

