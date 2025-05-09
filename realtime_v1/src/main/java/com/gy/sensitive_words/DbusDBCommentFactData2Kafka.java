package com.gy.sensitive_words;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.gy.sensitive_words.func.AsyncHbaseDimBaseDicFunc;
import com.gy.sensitive_words.func.IntervalJoinOrderCommentAndOrderInfoFunc;
import com.gy.stream.utils.CommonGenerateTempLate;
import com.gy.stream.utils.SensitiveWordsUtils;
import com.gy.utils.ConfigUtils;
import com.gy.utils.DateTimeUtils;
import com.gy.utils.EnvironmentSettingUtils;
import com.gy.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.retailersv1.DbusDBCommentFactData2Kafka
 * @Author zhou.han
 * @Date 2025/3/15 18:44
 * @description: Read MySQL CDC binlog data to kafka topics Task-03
 * TODO 该任务，修复了之前SQL的代码逻辑，在之前的逻辑中使用了FlinkSQL的方法进行了实现，把去重的问题，留给了下游的DWS，这种行为非常的yc
 * TODO Before FlinkSQL Left join and use hbase look up join func ,left join 产生的2条异常数据，会在下游做处理，一条为null，一条为未关联上的数据
 * TODO After FlinkAPI Async and google guava cache
 * Demo Data
 * 1 null
 * null
 * 1,1.1
 */
public class DbusDBCommentFactData2Kafka {

    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(15);

//        // 评论表 取数
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics("stream_retail_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaCdcDbSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


//        kafkaCdcDbSource.print();
//        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
//                KafkaUtils.buildKafkaSecureSource(
//                        kafka_botstrap_servers,
//                        kafka_cdc_db_topic,
//                        new Date().toString(),
//                        OffsetsInitializer.earliest()
//                ),
//                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner((event, timestamp) -> {
//                            if (event != null){
//                                try {
//                                    return JSONObject.parseObject(event).getLong("ts_ms");
//                                }catch (Exception e){
//                                    e.printStackTrace();
//                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
//                                    return 0L;
//                                }
//                            }
//                            return 0L;
//                        }
//                        ),
//                "kafka_cdc_db_source"
//        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        // 订单主表
        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");
        // 评论表进行进行升维处理 和hbase的维度进行关联补充维度数据
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));
//        {"op":"c","after":{"create_time":1746530012000,"user_id":695,"appraise":"1201","comment_txt":"评论内容：32275618265338265829374368637213422923954648182142","nick_name":"阿义","sku_id":15,"id":262,"spu_id":4,"order_id":2485},"source":{"thread":660,"server_id":1,"version":"1.9.7.Final","file":"mysql_bin.000017","connector":"mysql","pos":19585055,"name":"mysql_binlog_source","row":0,"ts_ms":1746501827000,"snapshot":"false","db":"stream_retail","table":"comment_info"},"ts_ms":1746501827560}
        //进行异步io的链接hbase表
        DataStream<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        100
                ).uid("async_hbase_dim_base_dic_func")
                .name("async_hbase_dim_base_dic_func");
//        enrichedStream.print();

        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject resJsonObj = new JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            resJsonObj.put("ts_ms", tsMs);
                            resJsonObj.put("db", dbName);
                            resJsonObj.put("table", tableName);
                            resJsonObj.put("server_id", serverId);
                            resJsonObj.put("appraise", after.getString("appraise"));
                            resJsonObj.put("commentTxt", after.getString("comment_txt"));
                            resJsonObj.put("op", jsonObject.getString("op"));
                            resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                            resJsonObj.put("create_time", after.getLong("create_time"));
                            resJsonObj.put("user_id", after.getLong("user_id"));
                            resJsonObj.put("sku_id", after.getLong("sku_id"));
                            resJsonObj.put("id", after.getLong("id"));
                            resJsonObj.put("spu_id", after.getLong("spu_id"));
                            resJsonObj.put("order_id", after.getLong("order_id"));
                            resJsonObj.put("dic_name", after.getString("dic_name"));
                            return resJsonObj;
                        }
                        return null;
                    }
                })
                .uid("map-order_comment_data")
                .name("map-order_comment_data");
//        orderCommentMap.print();
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map-order_info_data").name("map-order_info_data");
//        orderInfoMapDs.print();

//         orderCommentMap.order_id join orderInfoMapDs.id
        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));


        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
                .uid("interval_join_order_comment_and_order_info_func").name("interval_join_order_comment_and_order_info_func");

        //连接硅基流动,生成敏感词,与commentTxt进行拼接
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        }).uid("map-generate_comment").name("map-generate_comment");
//        supplementDataMap.print();
        //读取上流的数据,按20%的概率去再评论字段后面随机加上一句敏感词,
        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            //创建随机的一个方法
            private transient Random random;

            @Override
            public void open(Configuration parameters){
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject){
                //随机生成0.0到1.0(不包括)之间的随机的双精度小数,如果小于1.0则执行if判断里面的数据
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("map-sensitive-words").name("map-sensitive-words");

//        suppleMapDs.print();

        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        }).uid("add json ds").name("add json ds");
//        suppleTimeFieldDs.print();

//        suppleTimeFieldDs.map(js -> js.toJSONString())
//                .sinkTo(
//                KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_db_fact_comment_topic)
//        ).uid("kafka_db_fact_comment_sink").name("kafka_db_fact_comment_sink");


        SingleOutputStreamOperator<String> suppleTimeFieldSInkDs = suppleTimeFieldDs.map(jp -> jp.toJSONString());
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("realtime_v2_fact_comment_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        suppleTimeFieldSInkDs.sinkTo(sink);
        env.execute();
    }
}