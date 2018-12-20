package com.sunvalley.hadoop.util;

import com.sunvalley.hadoop.reduce.mapper.*;
import com.sunvalley.hadoop.reduce.model.SortModel;
import com.sunvalley.hadoop.reduce.model.StaffModel;
import com.sunvalley.hadoop.reduce.model.StaffProvincePartitioner;
import com.sunvalley.hadoop.reduce.reducer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    public static void getWordCountJobsConf(String jobName, String inputPath, String outputPath) throws IOException , ClassNotFoundException, InterruptedException {
        Configuration conf = getConfiguration();
        Job job = Job.getInstance(conf, jobName);

        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 小文件合并设置
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 最大分片
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 最小分片
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    /**
     * 单词统计
     * @param jobName
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void wordCount(String jobName, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConfiguration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(WordCount.class);

        // 指定Mapper的类
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        // 指定reduce的类
        job.setReducerClass(WordCount.IntSumReducer.class);

        // 设置Mapper输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 小文件合并设置
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 最大分片
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 最小分片
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1024);

        // 指定输入文件的位置
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // 指定输入文件的位置
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 将job中的参数，提交到yarn中运行
        job.waitForCompletion(true);
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
        jobConf.setReducerClass(WeatherReduce.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        return jobConf;
    }

    /**
     * 员工统计，对象序列化
     * @param jobName
     * @param inputPath
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void staff(String jobName, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConfiguration();
        Job job = Job.getInstance(configuration, jobName);

        // 设置jar中的启动类，可以根据这个类找到相应的jar包
        job.setJarByClass(StaffModel.class);
        job.setMapperClass(StaffMap.class);
        job.setReducerClass(StaffReduce.class);

        // 指定数据分区规则，不是必须要的，根据业务需求分区，如果设置了reduce根据设置的规则输出成很多个文件，默认是一个。
        job.setPartitionerClass(StaffProvincePartitioner.class);
        // 设置相应的reducer数量，这个数量要与分区的大最数量一致
        job.setNumReduceTasks(7);

        // 设置Mapper输出的key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出的value类型
        job.setMapOutputValueClass(StaffModel.class);
        // 设置reduce输出的key类型
        job.setOutputKeyClass(Text.class);
        // 设置reduce输出的value类型
        job.setOutputValueClass(StaffModel.class);

        // 小文件合并设置
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 最大分片
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 最小分片
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1024);

        // 指定输入文件的位置
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // 指定输入文件的位置
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    /**
     * 员工统计，带排序的对象序列化
     * @param jobName
     * @param inputPath
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void sort(String jobName, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = getConfiguration();
        Job job = Job.getInstance(config, jobName);
        // 设置jar中的启动类，可以根据这个类找到相应的jar包
        job.setJarByClass(SortModel.class);

        job.setMapperClass(SortMap.class);
        job.setReducerClass(SortReduce.class);

        // 设置Mapper的输出
        job.setMapOutputKeyClass(SortModel.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reduce的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SortModel.class);

        // 小文件合并设置
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 最大分片
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 最小分片
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1024);

        // 指定输入输出文件的位置
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.waitForCompletion(true);
    }

    @PostConstruct
    public void getPath() {
        hdfsPath = this.path;
    }

    public static String getHdfsPath() {
        return hdfsPath;
    }
} 

