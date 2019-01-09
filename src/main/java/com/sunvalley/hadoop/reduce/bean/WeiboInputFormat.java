package com.sunvalley.hadoop.reduce.bean;

import com.sunvalley.hadoop.reduce.model.Weibo;
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
public class WeiboInputFormat extends FileInputFormat<Text, Weibo> {

    /**
     * isSplitable方法就是是否要切分文件，这个方法显示如果是压缩文件就不切分，非压缩文件就切分。
     * 如果不允许分割，则isSplitable==false，则将第一个block、文件目录、开始位置为0，长度为整个文件的长度封装到一个InputSplit，加入splits中
     * 如果文件长度不为0且支持分割，则isSplitable==true,获取block大小，默认是64MB方法显示如果是压缩文件就不切分，非压缩文件就切分。
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * isSplitable(),如果是压缩文件就不切分，整个文件封装到一个InputSplit
     * isSplitable(),如果是非压缩文件就切，切分64MB大小的一块一块，再封装到InputSplit
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, Weibo> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WeiboRecordReader();
    }
}

