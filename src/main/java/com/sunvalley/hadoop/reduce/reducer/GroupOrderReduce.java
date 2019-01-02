package com.sunvalley.hadoop.reduce.reducer;

import com.sunvalley.hadoop.reduce.model.GroupOrder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 分组统计reduce
 *
 * @author: logan.zou
 * @date: 2018-12-29 17:50
 */
public class GroupOrderReduce extends Reducer<GroupOrder, NullWritable, GroupOrder, NullWritable> {
    /**
     * 接收GroupOrderMapper的输出，内容是GroupOrder对象
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(GroupOrder key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

