package com.es.agg.demo;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 用于es group by 最终生成对象
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EsModel {

    private String key;
    private Object value;
    private EsModel parent;

    /**
     * 构建成对象
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T build(Class<T> clazz) {
        Map<String, Object> data = new HashMap<>();
        data.put(key, value);
        EsModel condition = this;
        do {
            data.put(noKeyWord(condition.key), condition.value);
            condition = condition.parent;
        } while (condition != null);
        return JSON.parseObject(JSON.toJSONString(data), clazz);
    }

    public static String noKeyWord(String filedName) {
        return filedName.replace(".keyword", "");
    }

}
