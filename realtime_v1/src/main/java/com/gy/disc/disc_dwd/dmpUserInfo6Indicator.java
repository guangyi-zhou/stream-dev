package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.disc.Constellation;
import com.gy.utils.finksink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.gy.disc.disc_dwd.dwd_user_info
 * @Author guangyi_zhou
 * @Date 2025/5/12 15:09
 * @description: 对用户进行过滤
 */
public class dmpUserInfo6Indicator extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dmpUserInfo6Indicator().start(10051, 1, "dwd_user_info:1", "dmp_db");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
//        kafkaStrDS.print();
        //在kafka数据中过滤出user_info数据
        //对字段

        SingleOutputStreamOperator<JSONObject> UserInfoTableData = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info".equals(jsonObj.getJSONObject("source").getString("table")))
                //把birthday变换成yyyy-MM-dd
                .map(jsonObject -> {
                    JSONObject after = jsonObject.getJSONObject("after");
                    if (jsonObject.getJSONObject("after").getLong("id") != null) {
                        if (after != null && after.containsKey("birthday")) {
                            Integer birthday = after.getInteger("birthday");
                            if (birthday != null) {
                                LocalDate date = LocalDate.ofEpochDay(birthday);
                                after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                            }
                        }
                    }
                    return jsonObject;
                });
        SingleOutputStreamOperator<JSONObject> userInfoSupMsgTableData = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info_sup_msg".equals(jsonObj.getJSONObject("source").getString("table")));
        //布隆过滤器
        SingleOutputStreamOperator<JSONObject> UserInfoBloomDs = UserInfoTableData.keyBy(jsonObj -> jsonObj.getJSONObject("after").getLong("id"))
                .filter(new FilterBloomDeduplicatorFunc_v2(1000000, 0.01));

        SingleOutputStreamOperator<JSONObject> UserInfo = UserInfoBloomDs.map(jsonObject -> {
            JSONObject result = new JSONObject();
            JSONObject after = jsonObject.getJSONObject("after");
            if (jsonObject.containsKey("after")) {
                result.put("uid", after.getString("id"));
                result.put("uname", after.getString("name"));
                result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                result.put("user_level", after.getString("user_level"));
                result.put("login_name", after.getString("login_name"));
                result.put("phone_num", after.getString("phone_num"));
                result.put("email", after.getString("email"));
                result.put("birthday", after.getString("birthday"));
                result.put("constellation", Constellation.getConstellation(after.getString("birthday")));
                result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                String ear = after.getString("birthday");
                Integer intValue = new Integer(ear.substring(0, 4));
                result.put("ear", (intValue / 10) * 10);
            }
            return result;
        });
        UserInfo.print();
//        对userInfoSupMsgTableData进行处理
//        userInfoSupMsgTableData.print();
        SingleOutputStreamOperator<JSONObject> userInfoSumMsgBean = userInfoSupMsgTableData.keyBy(data -> data.getJSONObject("after").getLong("uid")).map(jsonObject -> {
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
        });
//        userInfoSumMsgBean.print();
        // 让 UserInfo 和 userInfoSumMsgBean 进行join
        //TODO 5.关联身高体重
        SingleOutputStreamOperator<JSONObject> userInfoJoinUserSumMsgDs = UserInfo
                .keyBy(o -> o.getInteger("uid"))
                .intervalJoin(userInfoSumMsgBean.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {
                        j1.put("height", j2.getString("height"));
                        j1.put("unit_height", j2.getString("unit_height"));
                        j1.put("weight", j2.getString("weight"));
                        j1.put("unit_weight", j2.getString("unit_weight"));
                        collector.collect(j1);
                    }
                })
                .uid("intervalJoin")
                .name("intervalJoin");
//        SingleOutputStreamOperator<String> userInfoJoinUserSumMsgDsString = userInfoJoinUserSumMsgDs.map(data -> data.toJSONString());
//        userInfoJoinUserSumMsgDsString.sinkTo(finksink.getkafkasink("dmp_user_info"));
        //对订单表进行过滤
    }
}
