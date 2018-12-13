package com.sunvalley.hadoop.reduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 类或方法的功能描述 :TODO
 *
 * @author: logan.zou
 * @date: 2018-12-05 15:37
 */
public class WordCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable();
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        // 未使用分词器
        String line = value.toString();
        String[] lineArr = line.split(" ");
        for (String arr : lineArr) {
//            System.out.println("---------");
//            System.out.println(arr);
            word.set(arr);
            output.collect(word, one);
        }

//         使用分词器
//        byte[] btValue = value.getBytes();
//        InputStream inputStream = new ByteArrayInputStream(btValue);
//        Reader reader = new InputStreamReader(inputStream);
//        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
//        Lexeme lexeme;
//        while ((lexeme = ikSegmenter.next()) != null) {
//            // 打印全部分词
//            System.out.println("---------");
//            System.out.println(lexeme.getLexemeText());
//            word.set(lexeme.getLexemeText());
//            output.collect(word, one);
//        }
    }
}

