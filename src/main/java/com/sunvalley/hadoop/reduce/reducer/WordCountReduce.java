package com.sunvalley.hadoop.reduce.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类或方法的功能描述 : 统计单个字符出现的次数
 *
 * @author: logan.zou
 * @date: 2018-12-05 18:29
 */
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private String text = "孙权";
    private int textSum = 0;
    private List<String> textList = null;

    public WordCountReduce() {
        textList = new ArrayList<>();
        textList.add("曹操");
        textList.add("孙权");
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);

        String keyStr = key.toString();
        // 未使用分词器，需要根据map传过来的行内容检索并累加
//        boolean isHas = keyStr.contains(text);
//        if (isHas) {
//            textSum++;
//            System.out.println("============ " + text + " 统计分词为: " + textSum + " ============");
//        }

        // 使用分词器，内容已经被统计好了，直接输出即可
        if (textList.contains(keyStr)) {
            System.out.println("============ " + keyStr + " 统计分词为: " + sum + " ============");
        }
    }
}

