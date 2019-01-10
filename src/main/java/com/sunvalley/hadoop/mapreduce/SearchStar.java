package com.sunvalley.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 类或方法的功能描述 : 统计男女明星微博搜索指数
 * 使用到reduce 的分组(男女分组),排序聚合(搜索指数排序),分别得到男性和女性搜索指数最高的人
 * @author: logan.zou
 * @date: 2019-01-09 17:32
 */
public class SearchStar extends Configured implements Tool {
    // 男
    private static final String MALE = "male";
    // 女
    private static final String FEMALE = "female";
    // 分隔符\t
    private static String TAB_SEPARATOR = "\t";


    /**
     * 微博明细搜索指数mapper类，读取 /java/weibo/weibo2.txt 文件，内容格式 李易峰	male	32670
     */
    public static class SearchStarMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * 每次调用map解析一行数据
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(),0,value.getLength(),"GBK");
            String[] split = line.split(TAB_SEPARATOR);
            if (split != null && split.length >= 3) {
                // 性别
                String gender = split[1].trim();
                // 输出内容 姓名  微博指数
                String content = split[0] + TAB_SEPARATOR + split[2];
//            male  李易峰	32670
                context.write(new Text(gender), new Text(content));
            }
        }
    }

    /**
     * 文件分区，分组
     * mapper执行完后调用，mapper的输出内容格式 key = male, value = 李易峰   32670
     */
    public static class SearchStarPartitioner extends Partitioner<Text, Text> {
        private static Map<String, Integer> map = new HashMap<>();
        static {
            //这里给每一个性别一个分区
            map.put(MALE,0);
            map.put(FEMALE,1);
        }
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String sex = key.toString();
            if (numReduceTasks == 0) {
                return 0;
            }
//            // 男性的返回1，女性返回2，其它返回0
            Integer gender = map.get(sex);
            return gender == null ? 0 : gender;
        }
    }

    /**
     * 文件聚合统计,排序,对map端的输出结果先进行一次合并，减少网络输出
     * 执行在partitioner之后，reduce之前，经过partitioner之后文件可能分成3分，男性一份，女性一份，其它一份
     * 内容格式 key = male, value = 李易峰   32670
     */
    public static class SearchStarCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxHotIndex = Integer.MIN_VALUE;
            String name = "";

            for (Text value : values) {
                String line = value.toString();
                String[] split = line.split(TAB_SEPARATOR);
                if (split != null && split.length >= 2) {
                    // 微博搜索指数
                    int hotIndex = Integer.parseInt(split[1]);
                    // 得到搜索指数最高的人
                    if (hotIndex > maxHotIndex) {
                        // 姓名
                        name = split[0];
                        maxHotIndex = hotIndex;
                    }
                }
            }
//            周杰伦 周杰伦	42020
            context.write(new Text(name), new Text(name + TAB_SEPARATOR + maxHotIndex));
        }
    }

    /**
     * 统计微博男女明星男女搜索指数
     */
    public static class SearchStarReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxHotIndex = Integer.MIN_VALUE;
            String name = "";

            for (Text value : values) {
                String line = value.toString();
                String[] split = line.split(TAB_SEPARATOR);
                if (split != null && split.length >= 2) {
                    // 微博搜索指数
                    int hotIndex = Integer.parseInt(split[1]);
                    if (hotIndex > maxHotIndex) {
                        // 姓名
                        name = split[0];
                        maxHotIndex = hotIndex;
                    }
                }
            }
//            angelababy angelababy	55083
            context.write(new Text(name), new Text(name + TAB_SEPARATOR + maxHotIndex));
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
        Job job = Job.getInstance(conf, "searchStar");
        // 设置jar信息
        job.setJarByClass(SearchStar.class);

        // 设置reduce文件拆分个数
        job.setNumReduceTasks(2);

        // 设置mapper信息
        job.setMapperClass(SearchStar.SearchStarMapper.class);
        // 设置分组partitioner信息
        job.setPartitionerClass(SearchStar.SearchStarPartitioner.class);
        // 设置排序combiner信息
        job.setCombinerClass(SearchStar.SearchStarCombiner.class);
        // 设置reduce信息
        job.setReducerClass(SearchStar.SearchStarReduce.class);

        // 设置mapper和reduce的输出格式，如果相同则只需设置一个
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置fs文件地址
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 运行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] filePath = {
                "hdfs://192.168.2.2:9000/java/weibo/weibo2.txt",
                "hdfs://192.168.2.2:9000/output/weibo2"
        };

        int ec = ToolRunner.run(new Configuration(), new SearchStar(), filePath);
        System.exit(ec);
    }
}

