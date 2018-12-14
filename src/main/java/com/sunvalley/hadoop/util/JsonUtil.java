package com.sunvalley.hadoop.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunvalley.hadoop.vo.ApiMsgEnum;
import com.sunvalley.hadoop.vo.BaseReturnVO;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 类或方法的功能描述 :TODO
 *
 * @author: logan.zou
 * @date: 2018-11-28 18:41
 */
public class JsonUtil {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String RES_CODE = "resCode";
    private static final String RES_Data = "data";
    private static final String RES_DES = "resDes";

    public JsonUtil() {
    }

    public static <T> T fromObject(Object objs, Class<T> type) {
        try {
            String json = toJSON(objs);
            return fromJSON(json, type);
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public static <T> List<T> fromObjectArray(Object objs, Class<T> type) {
        try {
            String json = toJSON(objs);
            return fromJSONArray(json, type);
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public static <T> T fromFeign(String json, Class<T> type) {
        try {
            int resultCode = getInteger(json, "resCode");
            if (resultCode != ApiMsgEnum.OK.getResCode()) {
                String resDes = getString(json, "resDes");
                throw new Exception(resDes);
            } else {
                T obj = fromJSON(json, "data", type);
                return obj;
            }
        } catch (Exception var5) {
            throw new RuntimeException(var5);
        }
    }

    public static <T> List<T> fromFeignArray(String jsonStr, Class<T> elementClass) {
        try {
            int resultCode = getInteger(jsonStr, "resCode");
            if (resultCode != ApiMsgEnum.OK.getResCode()) {
                String resDes = getString(jsonStr, "resDes");
                throw new Exception(resDes);
            } else {
                return fromJSONArray(jsonStr, "data", elementClass);
            }
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static <T> List<T> fromJSONArray(String jsonStr, String fieldName, Class<T> elementClass) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode jsonNode = node.get(fieldName);
            if (jsonNode == null) {
                return null;
            } else {
                String fieldJson = node.get(fieldName).toString();
                JavaType javaType = objectMapper.getTypeFactory().constructParametricType(ArrayList.class, new Class[]{elementClass});
                return (List)objectMapper.readValue(fieldJson, javaType);
            }
        } catch (Exception var7) {
            throw new RuntimeException(var7);
        }
    }

    public static <T> List<T> fromJSONArray(String jsonStr, Class<T> elementClass) {
        try {
            JavaType javaType = objectMapper.getTypeFactory().constructParametricType(ArrayList.class, new Class[]{elementClass});
            return (List)objectMapper.readValue(jsonStr, javaType);
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public static <T> T fromJSON(String jsonStr, String fieldName, Class<T> elementClass) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            String fieldJson = node.get(fieldName).toString();
            return objectMapper.readValue(fieldJson, elementClass);
        } catch (Exception var5) {
            throw new RuntimeException(var5);
        }
    }

    public static String getJsonString(String jsonStr, String fieldName) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode fieldJson = node.get(fieldName);
            return fieldJson == null ? "" : node.get(fieldName).toString();
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static String getString(String jsonStr, String fieldName) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode fieldJson = node.get(fieldName);
            return fieldJson == null ? "" : node.get(fieldName).textValue();
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static Integer getInteger(String jsonStr, String fieldName) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode jsonNode = node.get(fieldName);
            return jsonNode == null ? null : castToInt(jsonNode.asText());
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static Integer getInteger(String jsonStr, String fieldName, int defVal) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode jsonNode = node.get(fieldName);
            return jsonNode == null ? defVal : castToInt(jsonNode.asText());
        } catch (Exception var5) {
            return defVal;
        }
    }

    public static Long getLong(String jsonStr, String fieldName) {
        try {
            JsonNode node = objectMapper.readTree(jsonStr);
            JsonNode jsonNode = node.get(fieldName);
            return jsonNode == null ? null : castToLong(jsonNode.asText());
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static <T> T fromJSON(String json, Class<T> type) {
        try {
            T obj = objectMapper.readValue(json, type);
            return obj;
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static <T> T fromJSON(BaseReturnVO baseReturnVO, Class<T> type) {
        try {
            String json = toJson(baseReturnVO.getData());
            T obj = objectMapper.readValue(json, type);
            return obj;
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public static <T> T fromJSON(String source, TypeReference<T> ref) throws Exception {
        Object rtn = null;

        try {
            rtn = objectMapper.readValue(source, ref);
            return (T) rtn;
        } catch (IOException var4) {
            throw var4;
        }
    }

    public static <T> List<T> fromJSON(String jsonStr, Class<?> collectionClass, Class<T> elementClass) throws Exception {
        try {
            JavaType javaType = objectMapper.getTypeFactory().constructParametricType(collectionClass, new Class[]{elementClass});
            return (List)objectMapper.readValue(jsonStr, javaType);
        } catch (Exception var4) {
            throw var4;
        }
    }

    public static <T> String toJSON(T obj) {
        try {
            String jsonStr = objectMapper.writeValueAsString(obj);
            return jsonStr;
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public static String toJson(Object object) throws Exception {
        return objectMapper.writeValueAsString(object);
    }

    private static Integer castToInt(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return (Integer)value;
        } else if (value instanceof Number) {
            return ((Number)value).intValue();
        } else if (value instanceof String) {
            String strVal = (String)value;
            if (strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal)) {
                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                return Integer.parseInt(strVal);
            } else {
                return null;
            }
        } else {
            return value instanceof Boolean ? (Boolean)value ? 1 : 0 : null;
        }
    }

    private static Long castToLong(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).longValue();
        } else if (value instanceof String) {
            String strVal = (String)value;
            if (strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal)) {
                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                try {
                    return Long.parseLong(strVal);
                } catch (NumberFormatException var3) {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }
}

