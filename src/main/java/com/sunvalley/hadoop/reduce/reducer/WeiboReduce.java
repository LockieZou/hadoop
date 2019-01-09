package com.sunvalley.hadoop.reduce.reducer;

import com.sunvalley.hadoop.reduce.model.Weibo;
import com.sunvalley.hadoop.util.JsonUtil;
import com.sunvalley.hadoop.util.SortUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 类或方法的功能描述 : 明星微博统计
 *
 * @author: logan.zou
 * @date: 2019-01-04 16:05
 */
public class WeiboReduce extends Reducer<Text, Text, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> outputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs<>(context);
    }

    private Text text = new Text();

    /**
     * 读取 WeiboMapper的输出，内容格式 key=friends, value= 姚晨	627 ...
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 将输出内容放到map中
        Map<String, Integer> map = new HashMap<>();
        for (Text value : values) {
            String[] spilt = value.toString().split("\t");
            // 将数据放到map中
            map.put(spilt[0], Integer.parseInt(spilt[1].toString()));
        }

        // 对map内容排序
        Map.Entry<String, Integer>[] entries = SortUtil.sortHashMapByValue(map);
        // map排序后格式  [{"陈坤":73343207},{"姚晨":71382446}...]
        for (Map.Entry<String, Integer> entry : entries) {
            // 份文件输出，格式: friends 陈坤 73343207
            outputs.write(key.toString(), entry.getKey(), entry.getValue());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }
}

