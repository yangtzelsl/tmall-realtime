package com.yangtzelsl.tmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static volatile JedisPool jedisPool;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    //最大可用连接数
                    poolConfig.setMaxTotal(100);
                    //连接耗尽是否等待
                    poolConfig.setBlockWhenExhausted(true);
                    //等待时间
                    poolConfig.setMaxWaitMillis(2000);
                    //最大闲置连接数
                    poolConfig.setMaxIdle(5);
                    //最小闲置连接数
                    poolConfig.setMinIdle(5);
                    //取连接的时候进行一下测试 ping pong
                    poolConfig.setTestOnBorrow(true);
                    jedisPool = new JedisPool(poolConfig, "hd1", 6379, 1000, "密码");
                }
            }
        }
        return jedisPool.getResource();
    }

}

