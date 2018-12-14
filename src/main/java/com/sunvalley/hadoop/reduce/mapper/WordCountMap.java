package com.sunvalley.hadoop.reduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 类或方法的功能描述 :统计单个字符出现的次数
 *
 * @author: logan.zou
 * @date: 2018-12-05 15:37
 */
public class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        // 防止中文乱码
        String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
//        // 未使用分词器，分隔文件行默认为空格，也就是一行的内容作为key，value就是数量1即<行内容,1>
//        StringTokenizer itr = new StringTokenizer(line);
//        while (itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }

        // 使用分词器，分隔文件行内容根据常用的短语分隔，比如我们，被分隔成 <我,1>,<们,1><我们,1>
        byte[] btValue = line.getBytes();
        InputStream inputStream = new ByteArrayInputStream(btValue);
        Reader reader = new InputStreamReader(inputStream);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme lexeme;
        while ((lexeme = ikSegmenter.next()) != null) {
            word.set(lexeme.getLexemeText());
            context.write(word, one);
        }
    }
}

