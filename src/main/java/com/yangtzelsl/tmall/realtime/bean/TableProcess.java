package com.yangtzelsl.tmall.realtime.bean;

import lombok.Data;

@Data
public class TableProcess {

    /**
     * 动态分流Sink常量   改为小写和脚本一致
     */
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    /**
     * 来源表
     */
    private String sourceTable;

    /**
     * 操作类型 insert,update,delete
     */
    private String operateType;

    /**
     * 输出类型 hbase kafka
     */
    private String sinkType;

    /**
     * 输出表(主题)
     */
    private String sinkTable;

    /**
     * 输出字段
     */
    private String sinkColumns;

    /**
     * 主键字段
     */
    private String sinkPk;

    /**
     * 建表扩展
     */
    private String sinkExtend;
}

