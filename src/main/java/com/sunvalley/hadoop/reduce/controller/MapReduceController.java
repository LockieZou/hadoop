package com.sunvalley.hadoop.reduce.controller;

import com.sunvalley.hadoop.vo.BaseReturnVO;
import com.sunvalley.hadoop.reduce.service.MapReduceService;
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
     * 单词统计
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/newWordCount")
    public BaseReturnVO newWordCount(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.newWordCount(jobName, inputPath);
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

    /**
     * 员工统计,对象序列化
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/staff")
    public BaseReturnVO staff(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.staff(jobName, inputPath);
        return new BaseReturnVO("员工统计成功");
    }

    /**
     * 员工统计,带排序的对象序列化
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/sort")
    public BaseReturnVO sort(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.sort(jobName, inputPath);
        return new BaseReturnVO("员工统计排序成功");
    }

    /**
     * mapreduce 表join操作
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/join")
    public BaseReturnVO join(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.join(jobName, inputPath);
        return new BaseReturnVO("表join操作成功");
    }

    /**
     * mapreduce 获取共同好友
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/friends1")
    public BaseReturnVO friends1(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.friends1(jobName, inputPath);
        return new BaseReturnVO("获取共同好友成功");
    }

    /**
     * mapreduce 计算共同好友， input 为friends1的输出目录/output/friends1/part-r-0000
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/friends2")
    public BaseReturnVO friends2(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.friends2(jobName, inputPath);
        return new BaseReturnVO("计算共同好友成功");
    }

    /**
     * mapreduce 分组统计
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/groupOrder")
    public BaseReturnVO groupOrder(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.groupOrder(jobName, inputPath);
        return new BaseReturnVO("分组统计成功");
    }

    /**
     * mapreduce 带计数器的单词统计
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/counter")
    public BaseReturnVO counter(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.counter(jobName, inputPath);
        return new BaseReturnVO("计数器统计成功");
    }

    /**
     * mapreduce 明星微博统计
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping("/weibo")
    public BaseReturnVO weibo(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new BaseReturnVO("请求参数为空");
        }
        mapReduceService.weibo(jobName, inputPath);
        return new BaseReturnVO("明星微博统计成功");
    }
}

