package com.sunvalley.hadoop.reduce.bean;

import com.sunvalley.hadoop.reduce.model.GroupOrder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 类或方法的功能描述 :
 * 利用reduce端的GroupingComparator来实现将一组bean看成相同的key
 * @author: logan.zou
 * @date: 2018-12-29 17:45
 */
public class GroupOrderComparator extends WritableComparator {

    /**
     * 这个类必须写，因为mapreduce需要知道反射成为哪个类
     */
    public GroupOrderComparator() {
        super(GroupOrder.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GroupOrder o1 = (GroupOrder) a;
        GroupOrder o2 = (GroupOrder) b;
        //比较两个bean时，只比较这里面的一个字段，如果这里是相等的，那么mapreduce就会认为这两个对象是同一个key
        return o1.getItemid().compareTo(o2.getItemid());
    }
}

