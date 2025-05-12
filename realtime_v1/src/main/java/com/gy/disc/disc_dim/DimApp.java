package com.gy.disc.disc_dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.bean.CommonTable;
import com.gy.constat.constat;
import com.gy.realtime_dim.flinkfcation.Tablepeocessfcation;
import com.gy.realtime_dim.flinkfcation.flinksinkHbase;
import com.gy.realtime_dim.flinkfcation.flinksorceutil;
import com.gy.utils.Hbaseutli;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.gy.disc.disc_dim.DimApp
 * @Author guangyi_zhou
 * @Date 2025/5/12 13:40
 * @description: 对数据进简单的etl清晰, 然后把维度数据导入到Hbase
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        try {
            new DimApp().start(1051, 1, "ckAndGroupId", "dmp_db");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //对数据进行简单的etl处理
        SingleOutputStreamOperator<JSONObject> kafkaDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                String db = jsonObj.getJSONObject("source").getString("db");
                String type = jsonObj.getString("op");
                String data = jsonObj.getString("after");
                if ("realtime".equals(db)
                        && ("c".equals(type)
                        || "u".equals(type)
                        || "d".equals(type)
                        || "r".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    collector.collect(jsonObj);
                }
            }
        });
//        kafkaDs.print();
        //使用flink-cdc获取数据
        MySqlSource<String> getmysqlsource = flinksorceutil.getmysqlsource("stream_retail_config", "table_process_dim");
        DataStreamSource<String> mySQL_source = env.fromSource(getmysqlsource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);
        //把mysql的数据进行实体封装
        SingleOutputStreamOperator<CommonTable> tpds = mySQL_source.map(new MapFunction<String, CommonTable>() {
            @Override
            public CommonTable map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                CommonTable commonTable = null;
                if ("d".equals(op)) {
                    commonTable = jsonObject.getObject("before", CommonTable.class);
                } else {
                    commonTable = jsonObject.getObject("after", CommonTable.class);
                }
                commonTable.setOp(op);
                return commonTable;
            }
        });

//        tpds.print();


        SingleOutputStreamOperator<CommonTable> map = tpds.map(
                new RichMapFunction<CommonTable, CommonTable>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn = Hbaseutli.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseconn);
                    }
                    @Override
                    public CommonTable map(CommonTable commonTable) throws Exception {
                        String op = commonTable.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });
        // 创建一个描述符，用于定义状态映射，该映射以字符串类型为键，CommonTable类型为值
        MapStateDescriptor<String, CommonTable> tableMapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, CommonTable.class);
        BroadcastStream<CommonTable> broadcast = tpds.broadcast(tableMapStateDescriptor);

        BroadcastConnectedStream<JSONObject, CommonTable> connects = kafkaDs.connect(broadcast);
        //处理流合并
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable>> dimDS = connects.process(
                new Tablepeocessfcation(tableMapStateDescriptor)
        );
//        dimDS.print("dimDs");
        dimDS.addSink(new flinksinkHbase());


    }

}
