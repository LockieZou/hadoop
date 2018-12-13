package com.sunvalley.hadoop.reduce.service;

import com.sunvalley.hadoop.util.DateUtil;
import com.sunvalley.hadoop.util.HdfsUtil;
import com.sunvalley.hadoop.util.ReduceJobsUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.UUID;

/**
 * 类或方法的功能描述 : 单词统计
 *
 * @author: logan.zou
 * @date: 2018-12-05 19:02
 */
@Service
public class MapReduceService {
    // 默认reduce输出目录
    private static final String OUTPUT_PATH = "/output";
    /**
     * 单词统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void wordCount(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job,如果输出路径存在则删除，保证每次都是最新的
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        JobConf jobConf = ReduceJobsUtils.getWordCountJobsConf(jobName);
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        JobClient.runJob(jobConf);
    }

    /**
     * 一年最高气温统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void weather(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        JobConf jobConf = ReduceJobsUtils.getWeatherJobsConf(jobName);
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        JobClient.runJob(jobConf);
    }
} 

