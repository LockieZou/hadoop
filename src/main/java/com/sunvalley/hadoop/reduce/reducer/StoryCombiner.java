package com.sunvalley.hadoop.reduce.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 类或方法的功能描述 : 统计累加，并自定义排序
 * 合并统计类
 * @author: logan.zou
 * @date: 2018-12-18 15:30
 */
public class StoryCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> iterator = values.iterator();
        long num = 0;
        while (iterator.hasNext()) {
            LongWritable lw = iterator.next();
            num += lw.get();
        }
        longWritable.set(num);
        context.write(key, longWritable);
    }
}

