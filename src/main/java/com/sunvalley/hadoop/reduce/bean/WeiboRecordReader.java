package com.sunvalley.hadoop.reduce.bean;

import com.sunvalley.hadoop.reduce.model.Weibo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.*;

/**
 * 类或方法的功能描述 : 小文件合并读取
 *
 * @author: logan.zou
 * @date: 2018-12-28 18:33
 */
public class WeiboRecordReader extends RecordReader<Text, Weibo> {
    // 行读取器
    public LineReader in;
    // 每行数据类型
    public Text line;
    // 自定义key类型
    public Text lineKey;
    // 自定义value类型
    public Weibo lineValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取filesplit
        FileSplit fileSplit = (FileSplit) inputSplit;
        // 获取配置
        Configuration job = context.getConfiguration();
        // 分片路径
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(job);
        // 打开文件
        FSDataInputStream inputStream = fs.open(file);
        in = new LineReader(inputStream, job);
        line = new Text();
        lineKey = new Text();
        lineValue = new Weibo();
    }

    /**
     * 读取每行的数据，内容格式如下 唐嫣	唐嫣	    24301532	200	2391
     * 自定义输出key和value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 读取行内容，判断是否是空行
        int lineSize = in.readLine(line);
        if (lineSize == 0) {
            return false;
        }
        // 读取一行的内容并防止中文乱码
        String lineTmp = new String(line.getBytes(),0,line.getLength(),"GBK");
        String[] pieces = lineTmp.split("\t");
        if (pieces.length != 5) {
            throw new IOException("Invalid record received");
        }
        int friends, followers, num;
        try {
            // 粉丝数
            friends = Integer.parseInt(pieces[2]);
            // 关注数
            followers = Integer.parseInt(pieces[3]);
            // 微博数
            num = Integer.parseInt(pieces[4]);
        } catch (NumberFormatException nfe) {
            throw new IOException("Error parsing floation poing value in record");
        }

        // 自定义key和value
        lineKey.set(pieces[0]);
        lineValue.set(friends, followers, num);

        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return lineKey;
    }

    @Override
    public Weibo getCurrentValue() throws IOException, InterruptedException {
        return lineValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}

