package com.sunvalley.hadoop.reduce.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * 类或方法的功能描述 : 计算共同好友
 *
 * @author: logan.zou
 * @date: 2018-12-29 12:05
 */
public class FriendsMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * 计算共同好友读取的文件为FriendsReduce1 的输出结果，文件在 /output/friends1/part-1-0000
     * 内容格式 A	I,K,C,B,G,F,H,O,D,
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 根据tab分隔
        String[] split = line.split("\t");
        String friend = split[0];
        String[] personArr = split[1].split(",");
        // 排序后 B,C,D,F,G,H,I,K,O,
        Arrays.sort(personArr);

        // 把好友拆分成单个，如key为好友K，value为用户A
        for (int i = 0; i < personArr.length; i++) {
            System.out.println("====" + personArr[i]);
            for (int j = i + 1; j < personArr.length - 1; j++) {
                System.out.println("****" + personArr[j]);
                // 输出格式为 好友-用户
                context.write(new Text(personArr[i] + "-" + personArr[j]), new Text(friend));
                System.out.println(new Text(personArr[i] + "-" + personArr[j]));
            }
        }
    }
}

