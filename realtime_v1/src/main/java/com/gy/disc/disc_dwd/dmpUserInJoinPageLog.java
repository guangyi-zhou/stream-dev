package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.disc.*;
import com.gy.realtime_dim.flinkfcation.flinksorceutil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.gy.disc.disc_dwd.dmpUserInJoinPageLog
 * @Author guangyi_zhou
 * @Date 2025/5/13 10:25
 * @description:
 */
public class dmpUserInJoinPageLog {
    @SneakyThrows
    public static void main(String[] args) {
//        new dwd_user_info().start(10052, 1, "dwd_user_info", "dwd_traffic_page");\
        // 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10052);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        //创建消费者对象
        KafkaSource<String> source = flinksorceutil.getkafkasorce("dwd_traffic_page");

//        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaSource.print();
        //消费数据 封装为流
        DataStreamSource<String> kafkaSource = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null) {
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts");
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ), "dwd_traffic_page");

//        kafkaSource.print();

        SingleOutputStreamOperator<JSONObject> PageLogDateStream = kafkaSource.map(JSON::parseObject).map(new pageLogMapFunction());
//        PageLogDateStream.print();
        //布隆过滤器
        SingleOutputStreamOperator<JSONObject> BloomfilterPageLogDS = PageLogDateStream.keyBy(data -> data.getLong("uid"))
                .filter(new FilterBloomDeduplicatorFunc_v3(1000000, 0.01));
//        BloomfilterPageLogDS.print();
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = BloomfilterPageLogDS.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());

//        processStagePageLogDs.print("page去重后");
        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

        win2MinutesPageLogsDs.print("page的数据合并数组");





        env.execute();
    }
}
