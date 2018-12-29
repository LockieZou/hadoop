package com.sunvalley.hadoop.reduce.reducer;

import com.sunvalley.hadoop.reduce.model.SortModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 带排序的对象序列化reduce
 *
 * @author: logan.zou
 * @date: 2018-12-17 16:59
 */
public class SortReduce extends Reducer<SortModel, Text, Text, SortModel> {
    /**
     * 读取 sortMap 输出内容 内容格式 sortModel name
     *  因为在这之前已经是汇总的结果了，所以这里直接输出就行了
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(SortModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}

