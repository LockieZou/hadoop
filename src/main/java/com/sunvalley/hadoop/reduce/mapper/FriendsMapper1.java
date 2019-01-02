package com.sunvalley.hadoop.reduce.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 查找共同的好友
 *
 * @author: logan.zou
 * @date: 2018-12-29 10:47
 */
public class FriendsMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    /**
     * 读取 friends.txt 内容格式 A:B,C,D,F,E,O
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        // 根据冒号拆分
        String[] personFriends = line.split(":");
        // 第一个为用户
        String person = personFriends[0];
        // 第二个为好友
        String friends = personFriends[1];
        // 好友根据逗号拆分
        String[] friendsList = friends.split(",");
        for (String friend : friendsList) {
            k.set(friend);
            v.set(person);
            context.write(k, v);
        }
    }
}

