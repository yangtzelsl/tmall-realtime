package com.yangtzelsl.tmall.realtime.common;

public class TmallConfig {

    /**
     * hbase 数据库
     */
    public static final String HBASE_SCHEMA = "TMALL_REALTIME";

    /**
     * phoenix 连接地址
     */
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hd1,hd2,hd3:2181";

    /**
     * clickhouse 连接地址
     */
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hd1:8123/tmall_realtime";

}

