package com.sunvalley.hadoop.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 * 类或方法的功能描述 : 排序工具
 *
 * @author: logan.zou
 * @date: 2019-01-04 18:30
 */
public class SortUtil {
    public static Map.Entry<String, Integer>[] sortHashMapByValue(Map<String, Integer> map) {
        Map.Entry<String, Integer>[] entries = (Map.Entry<String, Integer>[]) map.entrySet().toArray(new Map.Entry[0]);
        // 排序
        Arrays.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                Long key1 = Long.valueOf(o1.getValue().toString());
                Long key2 = Long.valueOf(o2.getValue().toString());
                return key2.compareTo(key1);
            }
        });
        return entries;
    }
} 

