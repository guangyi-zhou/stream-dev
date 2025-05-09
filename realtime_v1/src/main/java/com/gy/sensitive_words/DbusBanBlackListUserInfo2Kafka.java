package com.gy.sensitive_words;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.gy.constat.constat;
import com.gy.sensitive_words.func.FilterBloomDeduplicatorFunc;
import com.gy.sensitive_words.func.MapCheckRedisSensitiveWordsFunc;
import com.gy.utils.EnvironmentSettingUtils;
import com.gy.utils.KafkaUtils;
import com.gy.utils.finksink;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

/**
 * @Package com.gy.sensitive_words.DbusBanBlackListUserInfo2Kafka
 * @Author guangyi_zhou
 * @Date 2025/5/8 14:50
 * @description: 黑名单封禁 Task -04
 */
public class DbusBanBlackListUserInfo2Kafka {
//    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka_botstrap_servers");
//    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka_db_fact_comment_topic");
//    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka_result_sensitive_words_topic");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("realtime_v2_fact_comment_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaCdcDbSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaCdcDbSource.print();


//        kafkaCdcDbSource.print("kafka数据");

        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");
//        {"info_original_total_amount":"9928.00","info_activity_reduce_amount":"250.00",
//        "commentTxt":"索芙特i-Softto璀璨金钻哑光唇膏Y01复古红，保湿滋润不掉色，百搭气质。","info_province_id":30,"info_payment_way":"3501",
//        "ds":"20250506","info_refundable_time":1747134748000,"info_order_status":"1004","info_create_time":1746529948000,"id":262,
//        "spu_id":4,"table":"comment_info","info_operate_time":1746530012000,"info_tm_ms":1746501827583,"op":"c",
//        "create_time":1746530012000,"info_user_id":695,"info_op":"u","info_trade_body":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 等2件商品",
//        "sku_id":15,"server_id":"1","dic_name":"1201","info_consignee_tel":"13236426743","info_total_amount":"9678.00",
//        "info_out_trade_no":"678272671123544","appraise":"1201","user_id":695,"info_id":2485,"info_coupon_reduce_amount":"0.00",
//        "order_id":2485,"info_consignee":"郑义","ts_ms":1746501827560,"db":"stream_retail"}

//        mapJsonStr.print();

        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));
//        bloomFilterDs.print("布隆");
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");

//        SensitiveWordsDs.print();

        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        }).uid("second sensitive word check").name("second sensitive word check");

        secondCheckMap.print();




        SingleOutputStreamOperator<String> secondCheckMapSInkDs = secondCheckMap.map(jp -> jp.toJSONString());

//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("realtime_v2_result_sensitive_words_user")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//
//        secondCheckMapSInkDs.sinkTo(sink);

        SingleOutputStreamOperator<String> map = secondCheckMap.map(JSON::toString);
//        map.print();
//        //写入doris
        map.sinkTo(finksink.getDorisSink("result_sensitive_words_user"));

        env.execute();
    }
}