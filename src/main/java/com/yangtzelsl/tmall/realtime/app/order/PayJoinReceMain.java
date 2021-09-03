package com.yangtzelsl.tmall.realtime.app.order;

import com.yangtzelsl.tmall.realtime.bean.OrderEvent;
import com.yangtzelsl.tmall.realtime.bean.ReceiptEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用 table api 处理
 * 由于是相对关联，因此匹配度不是很高；
 * TableAPI 只能实现符合需求的数据输出，不能输出不符合的数据。
 */
public class PayJoinReceMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1. 读取订单事件数据
        DataStream<String> inputOrderStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\my-gitlib\\shishi-daping\\dip\\shishi-daping\\NFDWSYYBigScreen\\TestJsonDmon\\src\\main\\resources\\OrderLog.csv");
        KeyedStream<OrderEvent, String> orderDataStream = inputOrderStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] dataArray = s.split(",");
                return new OrderEvent(Long.parseLong(dataArray[0]), dataArray[1], dataArray[2], Long.parseLong(dataArray[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        }).filter(order -> order.getAction().equals("pay"))
                .keyBy(order -> order.getOrId());

        // 2. 读取到账事件数据
        DataStream<String> inputReceipStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\my-gitlib\\shishi-daping\\dip\\shishi-daping\\NFDWSYYBigScreen\\TestJsonDmon\\src\\main\\resources\\ReceiptLog.csv");
        KeyedStream<ReceiptEvent, String> receipDataStream = inputReceipStream.map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String s) throws Exception {
                String[] dataArray = s.split(",");
                return new ReceiptEvent(dataArray[0], dataArray[1], Long.parseLong(dataArray[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ReceiptEvent element) {
                return element.getTimestamp() * 1000L;
            }
        }).keyBy(order -> order.getOrId());

        // -------------------------------关联处理-------------------------------------------------

        DataStream resultStream = orderDataStream.intervalJoin(receipDataStream)  //这里使用相对关联
                .between(Time.seconds(-3), Time.seconds(5))  // 订单数据等待到账数据时间前三秒到后三秒区间
                .process(new OrderMatchWithJoinFunction());  // 自定义类输出服务上边条件的数据

        // ---------------------------------------------------------------------------------------

        resultStream.print();
        env.execute("tx match with join job");
    }

    public static class OrderMatchWithJoinFunction extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receiptEvent));
        }
    }
}


