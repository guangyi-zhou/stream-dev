package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.disc.FilterBloomDeduplicatorFunc_v3;
import com.gy.disc.FlinkDeduplicationUtil;
import com.gy.disc.pageLogMapFunction;
import com.gy.realtime_dim.flinkfcation.flinksorceutil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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

        SingleOutputStreamOperator<JSONObject> maxTsPerUserDS = PageLogDateStream
                .keyBy(data -> data.getLong("uid"))
                .process(new KeyedProcessFunction<Long, JSONObject, JSONObject>() {
                    private transient ValueState<JSONObject> maxTsRecordState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态：存储每个用户ID对应的最大时间戳记录
                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>(
                                "maxTsRecord",    // 状态名称
                                JSONObject.class  // 状态类型
                        );
                        maxTsRecordState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

                        // 获取当前记录的时间戳
                        long currentTs = jsonObject.getLong("ts");
                        // 获取当前存储的最大时间戳记录
                        JSONObject maxTsRecord = maxTsRecordState.value();

                        // 如果是第一条记录或当前记录时间戳更大，则更新状态
                        if (maxTsRecord == null || currentTs > maxTsRecord.getLong("ts")) {
                            maxTsRecordState.update(jsonObject);
                        }
                    }
                }).uid("maxTsPerUse ts")
                .name("maxTsPerUse ts"
);
        maxTsPerUserDS.print();
        env.execute();
    }
}
