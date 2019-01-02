package com.sunvalley.hadoop.reduce.mapper;

import com.sunvalley.hadoop.reduce.model.OrderInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 类或方法的功能描述 : mapreduce 表join功能
 *
 * @author: logan.zou
 * @date: 2018-12-20 15:47
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, OrderInfo> {
    private Text text = new Text();
    private OrderInfo orderInfo = new OrderInfo();
    private final static String ORDER_FILE_NAME = "order";
    private final static String PRODUCT_FILE_NAME = "product";
    private final static String ORDER_FLAG = "0";
    private final static String PRODUCT_FLAG = "1";

    /**
     * 读取 order.txt 内容格式 1001,20170822,p1,3
     * 读取 product.txt  内容格式 p1,防空火箭,1,20.2
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=new String(value.getBytes(),0,value.getLength(),"GBK");
        // 跳过标题，标题带有#号
        if (line.startsWith("#")) {
            return;
        }

//        //获取当前任务的输入切片，这个InputSplit是一个最上层抽象类，可以转换成FileSplit
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        //得到的是文件名，这里根据文件名来判断是哪一种类型的数据，得到的是order或者product
        String fileName = fileSplit.getPath().getName();

        //我们这里通过文件名判断是哪种数据
        String pid = "";
        String[] spilt = line.split(",");
        if (fileName.startsWith(ORDER_FILE_NAME)) {
            //加载订单内容，订单数据里面有 订单号，时间，产品ID，数量
            Integer orderId = Integer.parseInt(spilt[0]);
            String orderDate = spilt[1];
            pid = spilt[2];
            Integer amount = Integer.parseInt(spilt[3]);
//            set(Integer orderId, String orderDate, String pid, Integer amount, String pname, Integer categoryId, Double price, String flag)
            orderInfo.set(orderId, orderDate, pid, amount, "", 0, 0.0, ORDER_FLAG);
        } else {
            //加载产品内容，产品数据有 产品编号，产品名称，种类，价格
            pid = spilt[0];
            String pname = spilt[1];
            Integer categoryId = Integer.parseInt(spilt[2]);
            Double price = Double.valueOf(spilt[3]);
            orderInfo.set(0,"",pid,0,pname,categoryId,price,PRODUCT_FLAG);
        }
        text.set(pid);
        context.write(text, orderInfo);
    }
}

