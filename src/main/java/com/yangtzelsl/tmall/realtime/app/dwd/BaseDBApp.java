package com.yangtzelsl.tmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.google.common.collect.Sets;
import com.yangtzelsl.tmall.realtime.bean.TableProcess;
import com.yangtzelsl.tmall.realtime.common.TmallConfig;
import com.yangtzelsl.tmall.realtime.enums.CDCTypeEnum;
import com.yangtzelsl.tmall.realtime.utils.DimUtil;
import com.yangtzelsl.tmall.realtime.utils.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashSet;

public class BaseDBApp {

    /**
     * 业务数据 topic
     */
    private static final String TOPIC_BASE = "ods_base_db_m";
    private static final String BASE_GROUP_ID = "ods_dwd_base_log_db";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //检查点，省略

        //拿到数据流
        DataStreamSource<String> dataStreamSource = env.addSource(KafkaUtil.ofSource(TOPIC_BASE, BASE_GROUP_ID));
        //转化格式，ETL
        SingleOutputStreamOperator<JSONObject> filteredDs = dataStreamSource
                .map(JSON::parseObject)
                .filter(new RichFilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return StringUtils.isNotBlank(jsonObject.getString("table"))
                                && jsonObject.getJSONObject("data") != null;
                    }
                });
        // Flink CDC 读取配置流
        DataStreamSource<String> ruleSource = env.addSource(MySQLSource.<String>builder()
                .hostname("hadoop3")
                .port(3306)
                .username("root")
                .password("密码")
                .databaseList("tmall_realtime")
                .tableList("tmall_realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        Struct value = (Struct) sourceRecord.value();
                        /*
                         * Struct{
                         *      after=Struct{
                         *              source_table=111,
                         *              operate_type=tses,
                         *              sink_type=1,
                         *              sink_table=1111
                         *      },
                         *      source=Struct{
                         *              db=tmall_realtime,
                         *              table=table_process
                         *      },
                         *      op=c
                         *      }
                         */
                        Struct source = value.getStruct("source");

                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("database", source.getString("db"));
                        jsonObject.put("table", source.getString("table"));
                        jsonObject.put("type", CDCTypeEnum.of(value.getString("op")).toString().toLowerCase());
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        for (Field field : after.schema().fields()) {
                            data.put(field.name(), after.get(field.name()));
                        }
                        jsonObject.put("data", data);

                        collector.collect(jsonObject.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .startupOptions(StartupOptions.initial())
                .build()
        );

        //配置流状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);

        //广播配置流状态
        BroadcastStream<String> broadcast = ruleSource.broadcast(mapStateDescriptor);

        //定义侧输出流，存放DIM数据
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dim_tag") {
        };

        //正式处理数据
        SingleOutputStreamOperator<JSONObject> dwdDs = filteredDs
                //合并 配置流
                .connect(broadcast)
                .process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {

                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                        connection = DriverManager.getConnection(TmallConfig.PHOENIX_SERVER);
                    }

                    @Override
                    public void close() throws Exception {
                        connection.close();
                    }

                    /**
                     * 处理 ODS 数据流
                     * @param jsonObject
                     * @param readOnlyContext
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
                        //获取 配置流状态
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

                        String table = jsonObject.getString("table");
                        String type = jsonObject.getString("type");
                        // MaxWell 处理历史数据 insert 的操作类型是 bootstrap-insert 需要修正一些
                        if ("bootstrap-insert".equals(type)) {
                            type = "insert";
                            jsonObject.put("type", type);
                        }

                        //拿到配置
                        String key = table + ":" + type;
                        TableProcess tableProcess = broadcastState.get(key);

                        if (tableProcess != null) {
                            //目标表放进去
                            jsonObject.put("sink_table", tableProcess.getSinkTable());
                            jsonObject.put("sink_pk", tableProcess.getSinkPk());
                            //过滤字段
                            HashSet<String> columnSet = Sets.newHashSet(tableProcess.getSinkColumns().split(","));
                            jsonObject.getJSONObject("data").entrySet().removeIf(e -> !columnSet.contains(e.getKey()));
                            //发送位置
                            String sinkType = tableProcess.getSinkType();
                            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                                collector.collect(jsonObject);
                            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                                readOnlyContext.output(dimTag, jsonObject);
                            }
                        } else {
                            //没有配置
                            System.out.println("NO this Key in TableProcess" + key);
                        }
                    }

                    /**
                     * 处理 配置流 数据
                     * @param s
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        TableProcess tableProcess = jsonObject.getObject("data", TableProcess.class);
                        String sourceTable = tableProcess.getSourceTable();
                        String operateType = tableProcess.getOperateType();
                        String sinkType = tableProcess.getSinkType();
                        String sinkPk = StringUtils.defaultString(tableProcess.getSinkPk(), "id");
                        String sinkExt = StringUtils.defaultString(tableProcess.getSinkExtend());
                        String sinkTable = tableProcess.getSinkTable();
                        String sinkColumns = tableProcess.getSinkColumns();

                        //如果是维度数据，需要通过Phoenix创建表
                        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && CDCTypeEnum.INSERT.toString().toLowerCase().equals(operateType)) {
                            StringBuilder sql = new StringBuilder();
                            sql.append("create table if not exists ").append(TmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append(" ( ");
                            String[] columns = sinkColumns.split(",");
                            for (int i = 0; i < columns.length; i++) {
                                String column = columns[i];
                                if (sinkPk.equals(column)) {
                                    sql.append(column).append(" varchar primary key ");
                                } else {
                                    sql.append("info.").append(column).append(" varchar ");
                                }
                                if (i < columns.length - 1) {
                                    sql.append(" , ");
                                }
                            }
                            sql.append(" ) ")
                                    .append(sinkExt);
                            System.out.println(sql);
                            try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
                                preparedStatement.execute();
                            }
                        }

                        //写入状态进行广播
                        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        broadcastState.put(sourceTable + ":" + operateType, tableProcess);
                    }
                });

        //处理 DIM 侧输出流 存入 HBase
        dwdDs.getSideOutput(dimTag).addSink(new RichSinkFunction<JSONObject>() {

            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                connection = DriverManager.getConnection(TmallConfig.PHOENIX_SERVER);
            }

            @Override
            public void close() throws Exception {
                connection.close();
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                JSONObject data = value.getJSONObject("data");
                StringBuilder sql = new StringBuilder();
                //目标表名
                String sinkTable = value.getString("sink_table");
                sql.append("upsert into ").append(TmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append(" (")
                        .append(StringUtils.join(data.keySet(), ","))
                        .append(") ")
                        .append("values( '")
                        .append(StringUtils.join(data.values(), "','"))
                        .append("' ) ");
                //入库
                try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
                    preparedStatement.execute();
                    // 默认不自动提交，需要手动提交
                    connection.commit();
                }
                //删除缓存
                String type = value.getString("type");
                if ("update".equals(type) || "delete".equals(type)) {
                    String sinkPk = value.getString("sink_pk");
                    DimUtil.delDimCache(sinkTable, data.getString(sinkPk));
                }
            }
        });

        // 处理 DWD 主流数据，存入 Kafka
        dwdDs.addSink(KafkaUtil.ofSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sink_table"), jsonObject.getJSONObject("data").toJSONString().getBytes(StandardCharsets.UTF_8));
            }
        }));

        //执行
        env.execute("db_ods_to_dwd");
    }
}

