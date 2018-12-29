package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 类或方法的功能描述 : hadoop中的分区，是在mapper结束后的reducer中，所以下面的代码是
 * 在reducer时运行的，我们对不同的省份进行规则划分，比如说江西就是对应的1分区
 *
 * @author: logan.zou
 * @date: 2018-12-17 16:04
 *  key ,value是mapper中输出的类型，因为分区是在mapper完成之后进行的
 */
public class StaffProvincePartitioner extends Partitioner<Text, StaffModel> {
    private static Map<String, Integer> map = new HashMap<>();

    static {
        //这里给每一个省份编制一个分区
        map.put("江西",1);
        map.put("广东",2);
        map.put("北京",3);
        map.put("湖南",4);
        map.put("上海",5);
        map.put("西藏",6);
    }

    /**
     * 给指定的数据分区
     * @param text
     * @param staffModel
     * @param num
     * @return
     */
    @Override
    public int getPartition(Text text, StaffModel staffModel, int num) {
        // 根据key获取分区
        Integer province = map.get(staffModel.getProvince().toString());
        // 如果在省份列表中找不到，则指定一个默认的分区
        province = province == null ? 0 : province;
        return province;
    }
}

