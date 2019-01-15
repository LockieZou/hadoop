package com.sunvalley.hadoop.mapreduce;

import com.sunvalley.hadoop.reduce.model.GroupSortModel;
import com.sunvalley.hadoop.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 分组统计，并对value排序
 *
 * @author: logan.zou
 * @date: 2019-01-11 12:14
 */
public class GroupSort extends Configured implements Tool {

    /**
     * 分组统计排序mapper类
     * 读取 /java/groupSort.txt 文件，内容格式
     * 40	20
     * 30	20
     */
    public static class GroupSortMapper extends Mapper<LongWritable, Text, GroupSortModel, IntWritable> {
        private static final GroupSortModel groupSortModel = new GroupSortModel();
        private static final IntWritable num = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            if (split != null && split.length >= 2) {
                groupSortModel.set(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
                num.set(Integer.parseInt(split[1]));
//                {"name":40,"num":20} 20
//                System.out.println("mapper输出：" + JsonUtil.toJSON(groupSortModel) + " " + num);
                context.write(groupSortModel, num);
            }
        }
    }

    /**
     * 分区过滤
     */
    public static class GroupSortPartitioner extends Partitioner<GroupSortModel, IntWritable> {
        @Override
        public int getPartition(GroupSortModel key, IntWritable value, int numPartitions) {
            return Math.abs(key.getName() * 127) % numPartitions;
        }
    }

    /**
     * 统计
     */
    public static class GroupSortComparator extends WritableComparator {
        public GroupSortComparator() {
            super(GroupSortModel.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            GroupSortModel model = (GroupSortModel) a;
            int num = model.getNum();
            GroupSortModel model2 = (GroupSortModel) b;
            int num2 = model2.getNum();
//            comparator输出：20 1
//            System.out.println("comparator输出：" + model.getName() + " " + model.getNum());
//            comparator2输出：20 10
//            System.out.println("comparator2输出：" + model2.getName() + " " + model2.getNum());
            return num == num2 ? 0 : (num < num2 ? -1 : 1);
        }
    }

    /**
     * 分组统计
     */
    public static class GroupSortReduce extends Reducer<GroupSortModel, IntWritable, Text, IntWritable> {
        private static final Text name = new Text();
        @Override
        protected void reduce(GroupSortModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            name.set(key + "");
            for (IntWritable value : values) {
//                reduce输出：20	1 1
                System.out.println("reduce输出：" + key + " " + value);
                context.write(name, value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();

        // 如果目标文件存在则删除
        Path outPath = new Path(args[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            boolean flag = fs.delete(outPath, true);
        }

        // 新建一个Job
        Job job = Job.getInstance(conf, "groupSort");
        // 设置jar信息
        job.setJarByClass(GroupSort.class);

        // 设置mapper信息
        job.setMapperClass(GroupSort.GroupSortMapper.class);
        // 设置reduce信息
        job.setReducerClass(GroupSort.GroupSortReduce.class);

        // 设置mapper和reduce的输出格式，如果相同则只需设置一个
        job.setOutputKeyClass(GroupSortModel.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置fs文件地址
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 运行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] filePath = {
                "hdfs://192.168.2.2:9000/java/groupSort.txt",
                "hdfs://192.168.2.2:9000/output/groupSort"
        };

        int ec = ToolRunner.run(new Configuration(), new SearchStar(), filePath);
        System.exit(ec);
    }
}

