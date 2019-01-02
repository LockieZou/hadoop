package com.sunvalley.hadoop.reduce.bean;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 小文件合并格式化类
 *
 * @author: logan.zou
 * @date: 2018-12-28 18:31
 */
public class LitterFileInputFormat extends FileInputFormat<LongWritable, Text> {

    /**
     * 设置每个小文件不可分片，保证小文件生成一个key-value 键值对
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        LitterFileRecordReader reader = new LitterFileRecordReader();
        reader.initialize(inputSplit, context);
        return reader;
    }
}

