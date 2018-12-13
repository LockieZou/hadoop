package com.sunvalley.hadoop.util;

import com.sunvalley.hadoop.reduce.mapper.WeatherMap;
import com.sunvalley.hadoop.reduce.mapper.WordCountMap;
import com.sunvalley.hadoop.reduce.reducer.WeatherReduce;
import com.sunvalley.hadoop.reduce.reducer.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * 类或方法的功能描述 : Map/Reduce工具类
 *
 * @author: logan.zou
 * @date: 2018-12-04 14:16
 */
@Component
public class ReduceJobsUtils {
    @Value("${hdfs.path}")
    private String path;

    private static String hdfsPath;

    /**
     * 获取HDFS配置信息
     * @return
     */
    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        configuration.set("mapred.job.tracker", hdfsPath);
        return configuration;
    }

    /**
     * 获取单词统计的配置信息
     * @param jobName
     * @return
     */
    public static JobConf getWordCountJobsConf(String jobName) throws IOException {
        JobConf jobConf = new JobConf(getConfiguration());
        jobConf.setJobName(jobName);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setMapperClass(WordCountMap.class);
        jobConf.setCombinerClass(WordCountReduce.class);
        jobConf.setReducerClass(WordCountReduce.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        return jobConf;
    }

    /**
     * 获取单词一年最高气温计算配置
     * @param jobName
     * @return
     */
    public static JobConf getWeatherJobsConf(String jobName) {
        JobConf jobConf = new JobConf(getConfiguration());
        jobConf.setJobName(jobName);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setMapperClass(WeatherMap.class);
        jobConf.setCombinerClass(WeatherReduce.class);
        jobConf.setReducerClass(WeatherReduce.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        return jobConf;
    }


    @PostConstruct
    public void getPath() {
        hdfsPath = this.path;
    }

    public static String getHdfsPath() {
        return hdfsPath;
    }
} 

