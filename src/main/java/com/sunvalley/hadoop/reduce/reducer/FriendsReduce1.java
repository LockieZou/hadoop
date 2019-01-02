package com.sunvalley.hadoop.reduce.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 获取共同好友
 *
 * @author: logan.zou
 * @date: 2018-12-29 11:07
 */
public class FriendsReduce1 extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();


    /**
     * 读取 FriendsMapper1 输出，内容格式 B A
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        // 循环好友
        for (Text person : values) {
            sb.append(person).append(",");
        }
        k.set(key);
        v.set(sb.toString());
        context.write(k, v);
    }
}

