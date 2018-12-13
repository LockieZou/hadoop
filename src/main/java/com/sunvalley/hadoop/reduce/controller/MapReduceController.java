package com.sunvalley.hadoop.reduce.controller;

import com.sunvalley.hadoop.VO.BaseReturnVO;
import com.sunvalley.hadoop.reduce.service.MapReduceService;
import com.sunvalley.hadoop.util.HdfsUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 类或方法的功能描述 :TODO
 *
 * @author: logan.zou
 * @date: 2018-12-05 19:10
 */
@RestController
@RequestMapping("/hadoop/reduce")
public class MapReduceController {
    @Autowired
    MapReduceService mapReduceService;

    /**
     * 单词统计
     * @param jobName
     * @param inputPath
     * @return
     */
    @PostMapping("/wordCount")
    public BaseReturnVO wordCount(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.wordCount(jobName, inputPath);
        return new BaseReturnVO("单词统计成功");
    }

    /**
     * 一年最高气温统计
     * @param jobName
     * @param inputPath
     * @return
     */
    @PostMapping("/weather")
    public BaseReturnVO weather(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.weather(jobName, inputPath);
        return new BaseReturnVO("温度统计成功");
    }
} 

