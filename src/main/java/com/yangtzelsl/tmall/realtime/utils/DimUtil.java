package com.yangtzelsl.tmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yangtzelsl.tmall.realtime.common.TmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {

    /**
     * 删除缓存
     * @param tableName
     * @param id
     */
    public static void delDimCache(String tableName, String id) {
        StringBuilder cacheKey = new StringBuilder().append("dim:").append(tableName.toLowerCase()).append(":").append(id);
        try (Jedis jedis = RedisUtil.getJedis()) {
            jedis.del(cacheKey.toString());
        }
    }

    /**
     * 更具 表名 和 id 查询一条数据
     * @param tableName 表名
     * @param id id
     * @return  数据
     */
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    /**
     * 更具 表名 和 多个条件 查询一条数据
     * @param tableName 表名
     * @param colAndValue 必须的 （条件，值）
     * @param colAndValues 可选多个 （条件，值）
     * @return 数据
     */
    @SafeVarargs
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String> colAndValue, Tuple2<String, String>... colAndValues) {
        //缓存 key
        StringBuilder cacheKey = new StringBuilder().append("dim:").append(tableName.toLowerCase()).append(":").append(colAndValue.f1);
        for (Tuple2<String, String> cv : colAndValues) {
            cacheKey.append("_").append(cv.f1);
        }
        try (Jedis jedis = RedisUtil.getJedis()) {
            //查缓存
            String str = jedis.get(cacheKey.toString());
            if (StringUtils.isNotBlank(str)) {
                return JSON.parseObject(str);
            }
            //拼接sql
            StringBuilder sql = new StringBuilder();
            sql.append("select * from ").append(TmallConfig.HBASE_SCHEMA).append(".").append(tableName)
                    .append(" where ").append(colAndValue.f0).append("='").append(colAndValue.f1).append("' ");
            for (Tuple2<String, String> cv : colAndValues) {
                sql.append("and ").append(cv.f0).append("='").append(cv.f1).append("' ");
            }
            // 查询
            List<JSONObject> jsonObjectList = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
            if (!jsonObjectList.isEmpty()) {
                JSONObject jsonObject = jsonObjectList.get(0);
                jedis.setex(cacheKey.toString(), 60 * 60 * 24, jsonObject.toJSONString());
                return jsonObject;
            }
        }
        return null;
    }

}

