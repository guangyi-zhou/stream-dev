package com.gy.disc.disc_dim;

import com.gy.constat.constat;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.gy.disc.disc_dim.flinkCDC
 * @Author guangyi_zhou
 * @Date 2025/5/12 9:59
 * @description: flink_cdc采集业务数据写入topic
 */
public class flinkCDC {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(properties)
                .port(3306)
                .databaseList("stream_retail")
                .tableList("stream_retail.*")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())//将SourceRecord转换为JSON字符串
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);// 设置 sink 节点并行度为 1

//        {"before":null,"after":{"id":2944,"order_id":1504,"order_status":"1001","create_time":1744068523000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744100949000,"snapshot":"false","db":"realtime","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":2446435,"row":0,"thread":177,"query":null},"op":"c","ts_ms":1744100949425,"transaction":null}
        ds.print();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(constat.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("dmp_db")
//                                 realtime_v1_table_all_mysql_v2
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        ds.sinkTo(sink);
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
