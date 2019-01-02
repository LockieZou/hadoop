package com.sunvalley.hadoop.reduce.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 类或方法的功能描述 :计算共同好友
 *
 * @author: logan.zou
 * @date: 2018-12-29 13:49
 */
public class FriendsReduce2 extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    /**
     * 接收 FriendsMapper2 的文件输出，内容格式 I-K A
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text friend : values) {
            sb.append(friend.toString()).append(" ");
        }
        k.set(key);
        v.set(sb.toString());
        System.out.println(key + "----" + sb.toString());
        context.write(k, v);
    }
}

