package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.disc.ProcessJoinBase2And4BaseFunc;
import com.gy.disc.ProcessLabelFunc;
import com.gy.utils.EnvironmentSettingUtils;
import com.gy.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

/**
 * @Package com.label.DbusUserLabel6BaseCalculate
 * @Author guangyi_zhou
 * @Date 2025/5/15 15:32
 * @description:
 */
public class DbusUserLabel6BaseCalculate {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092,cdh02:9092,cdh03:9092",
                                "kafka_label_base6_topic",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base6_topic_source"
                ).uid("kafka_base6_source")
                .name("kafka_base6_source");

        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092,cdh02:9092,cdh03:9092",
                                "kafka_label_base4_topic",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");

        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092,cdh02:9092,cdh03:9092",
                                "kafka_label_base2_topic",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");

        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);
//        mapBase6LabelDs.print("mapBase6LabelDs");
//        mapBase4LabelDs.print("mapBase4LabelDs");
//        mapBase2LabelDs.print("mapBase2LabelDs");


        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.minutes(-20), Time.minutes(20))
                .process(new ProcessJoinBase2And4BaseFunc());

        join2_4Ds.print();

        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));

        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.minutes(-20), Time.minutes(20))
                .process(new ProcessLabelFunc());


//        userLabelProcessDs.print();


        env.execute();
    }

}
