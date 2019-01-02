package com.sunvalley.hadoop.reduce.bean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 小文件合并读取
 *
 * @author: logan.zou
 * @date: 2018-12-28 18:33
 */
public class LitterFileRecordReader extends RecordReader<LongWritable, Text> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private Text value = new Text();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream in = null;
             try {
                 in = fs.open(path);
                 // 把输入流上的数据全部读取到contents字节数组里
                 IOUtils.readFully(in, contents, 0, contents.length);
                 // 把读取到的数据设置到value里
                 value.set(contents, 0, contents.length);
             } finally {
                IOUtils.closeStream(in);
             }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}

