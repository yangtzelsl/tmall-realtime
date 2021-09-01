package com.yangtzelsl.tmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yangtzelsl.tmall.realtime.utils.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    /**
     * 所以日志数据
     */
    private static final String TOPIC_BASE = "ods_base_log";
    private static final String BASE_GROUP_ID = "ods_dwd_base_log_app";

    /**
     * 启动日志数据
     */
    private static final String TOPIC_START = "dwd_start_log";
    /**
     * 页面日志数据
     */
    private static final String TOPIC_PAGE = "dwd_page_log";
    /**
     * 曝光日志数据
     */
    private static final String TOPIC_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //省略检查点

        //拿到数据流
        DataStreamSource<String> dataStreamSource = env.addSource(KafkaUtil.ofSource(TOPIC_BASE, BASE_GROUP_ID));

        // 处理新用户字段，防止前端数据错误
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = dataStreamSource
                .map(JSON::parseObject)
                .keyBy(j -> j.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> newMidDateState;
                    private SimpleDateFormat yyyyMMdd;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        newMidDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                        yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        //1判断是不是新用户，是新用户，修复
                        if ("1".equals(isNew)) {
                            String newMidDate = newMidDateState.value();
                            String ts = yyyyMMdd.format(new Date(jsonObject.getLong("ts")));
                            if (StringUtils.isEmpty(newMidDate)) {
                                newMidDateState.update(ts);
                            } else {
                                if (!newMidDate.equals(ts)) {
                                    jsonObject.getJSONObject("common").put("is_new", "0");
                                }
                            }
                        }
                        return jsonObject;
                    }
                });

        //启动日志侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        //曝光日志侧输出流
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };

        // 判断 不同的日志类型 输出到各流
        SingleOutputStreamOperator<String> pageDStream = midWithNewFlagDS
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        JSONObject start = jsonObject.getJSONObject("start");
                        if (start != null) {
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            collector.collect(jsonObject.toJSONString());
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (!CollectionUtil.isNullOrEmpty(displays)) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displaysJsonObject = displays.getJSONObject(i);
                                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                    displaysJsonObject.put("page_id", pageId);
                                    context.output(displayTag, displaysJsonObject.toJSONString());
                                }
                            }
                        }
                    }
                });

        //写入 kafka
        pageDStream.addSink(KafkaUtil.ofSink(TOPIC_PAGE));
        pageDStream.getSideOutput(startTag).addSink(KafkaUtil.ofSink(TOPIC_START));
        pageDStream.getSideOutput(displayTag).addSink(KafkaUtil.ofSink(TOPIC_DISPLAY));

        env.execute("log_ods_to_dwd");
    }

}

