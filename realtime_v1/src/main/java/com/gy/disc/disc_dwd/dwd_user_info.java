package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.disc.Constellation;
import com.gy.utils.finksink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Year;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.gy.disc.disc_dwd.dwd_user_info
 * @Author guangyi_zhou
 * @Date 2025/5/12 15:09
 * @description: 对用户进行过滤
 */
public class dwd_user_info extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dwd_user_info().start(10051,1,"dwd_user_info:1","dmp_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {




//        kafkaStrDS.print();
        //在kafka数据中过滤出user_info数据
        SingleOutputStreamOperator<JSONObject> UserInfoDS = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info".equals(jsonObj.getJSONObject("source").getString("table")));

        SingleOutputStreamOperator<JSONObject> userInfoSupMsgDS = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info_sup_msg".equals(jsonObj.getJSONObject("source").getString("table")));
//        UserInfoDS.print();
//        {"op":"r","after":{"birthday":6000,"create_time":1746567349000,"login_name":"tyy1jb7rn","nick_name":"阿梁","name":"韦梁","user_level":"1","phone_num":"13671183453","id":421,"email":"tyy1jb7rn@googlemail.com"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"stream_retail","table":"user_info"},"ts_ms":1747016022079}
        //对字段


        //把birthday变换成yyyy-MM-dd
        SingleOutputStreamOperator<JSONObject> mapDs = UserInfoDS.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject after = jsonObject.getJSONObject("after");
                if(jsonObject.getJSONObject("after").getLong("id") != null){
                    if (after != null && after.containsKey("birthday")) {
                        Integer birthday = after.getInteger("birthday");
                        if (birthday != null) {
                            LocalDate date = LocalDate.ofEpochDay(birthday);
                            after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                        }
                    }
                }
                return jsonObject;
            }
        });
//        mapDs.print();
        SingleOutputStreamOperator<JSONObject> filterDs = mapDs.keyBy(jsonObj -> jsonObj.getJSONObject("after").getLong("id"))
                .filter(new FilterBloomDeduplicatorFunc_v2(1000000, 0.01));

        SingleOutputStreamOperator<JSONObject> UserInfo = filterDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                if (jsonObject.containsKey("after")) {
                    result.put("uid", after.getString("id"));
                    result.put("uname", after.getString("name"));
                    result.put("gender", after.getString("gender"));
                    result.put("user_level", after.getString("user_level"));
                    result.put("login_name", after.getString("login_name"));
                    result.put("phone_num", after.getString("phone_num"));
                    result.put("email", after.getString("email"));
                    result.put("birthday", after.getString("birthday"));
                    result.put("constellation", Constellation.getConstellation(after.getString("birthday")));
                    result.put("ts_ms", after.getLongValue("ts_ms"));
                    String ear = after.getString("birthday");
                    Integer intValue = new Integer(ear.substring(0, 4));
                    result.put("ear",(intValue /10)*10);


                }
                return result;
            }
        });
//        UserInfo.print();
// 对userInfoSupMsgDS进行处理
//        userInfoSupMsgDS.print();
        SingleOutputStreamOperator<JSONObject> userInfoSumMsgBean = userInfoSupMsgDS.keyBy(data -> data.getJSONObject("after").getLong("uid")).map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                if (jsonObject.containsKey("after")) {
                    result.put("uid", after.getString("uid"));
                    result.put("gender", after.getString("gender"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getString("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getLongValue("height"));
                }
                return result;
            }
        });
//        userInfoSumMsgBean.print();


        // 让 UserInfo 和 userInfoSumMsgBean 进行join
        //TODO 5.关联身高体重
        SingleOutputStreamOperator<JSONObject> name = UserInfo
                .keyBy(o -> o.getInteger("uid"))
                .intervalJoin(userInfoSumMsgBean.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                            j1.put("height", j2.getString("height"));
                            j1.put("unit_height", j2.getString("unit_height"));
                            j1.put("weight", j2.getString("weight"));
                            j1.put("unit_weight", j2.getString("unit_weight"));
                            collector.collect(j1);
                    }
                })
                .uid("intervalJoin")
                .name("intervalJoin");
//        name.print();


        SingleOutputStreamOperator<String> map = name.map(data -> data.toJSONString());
        map.sinkTo(finksink.getkafkasink("dmp_user_info"));




        //对订单表进行过滤


    }
}
