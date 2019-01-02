package com.sunvalley.hadoop.reduce.reducer;

import com.sunvalley.hadoop.reduce.model.OrderInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类或方法的功能描述 : mapreduce的表join操作
 *
 * @author: logan.zou
 * @date: 2018-12-20 16:06
 */
public class JoinReduce extends Reducer<Text, OrderInfo, OrderInfo, NullWritable> {
    private final static String ORDER_FLAG = "0";
    private final static String PRODUCT_FLAG = "1";

    /**
     * 解析mapper读取后的文件格式 产品pid orderInfo对象
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<OrderInfo> values, Context context) throws IOException, InterruptedException {
        // 这个对象用来存放产品的数据，一个产品所以只有一个对象
        OrderInfo product = new OrderInfo();
        // 这个list用来存放所有的订单数据，订单肯定是有多个的
        List<OrderInfo> list = new ArrayList<>();

        // 循环map输出
        for (OrderInfo info : values) {
            // 判断是订单还是产品的map输出
            if (ORDER_FLAG.equals(info.getFlag())) {
                // 订单表数据
                OrderInfo tmp = new OrderInfo();
                try {
                    tmp = (OrderInfo) info.clone();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                list.add(tmp);
            } else {
                // 产品表数据
                try {
                    product = (OrderInfo) info.clone();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //经过上面的操作，就把订单与产品完全分离出来了，订单在list集合中，产品在单独的一个对象中
        //然后可以分别综合设置进去
        for (OrderInfo tmp : list) {
            tmp.setPname(product.getPname());
            tmp.setCategoryId(product.getCategoryId());
            tmp.setPrice(product.getPrice());
            // 最后输出
            context.write(tmp, NullWritable.get());
        }

    }
}

