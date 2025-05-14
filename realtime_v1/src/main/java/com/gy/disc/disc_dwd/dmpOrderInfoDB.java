package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.gy.disc.disc_dwd.dmpOrderInfoDB
 * @Author guangyi_zhou
 * @Date 2025/5/14 11:11
 * @description: orderInfoData
 */
public class dmpOrderInfoDB extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dmpUserInfo6Indicator().start(10053, 1, "dmpOrderInfoDB", "dmp_db");

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> orderInfoSupMsgTableData = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "order_info".equals(jsonObj.getJSONObject("source").getString("table")));
        orderInfoSupMsgTableData.print();

//        SingleOutputStreamOperator<JSONObject> mapDs = orderInfoSupMsgTableData.keyBy(data -> data.getLong("user_id"))
//                .map(jsonObject -> {
//                    JSONObject result = new JSONObject();
//                    if (jsonObject.containsKey("after")) {
//                        JSONObject after = jsonObject.getJSONObject("after");
//                        result.put("uid", jsonObject.getLong("user_id"));
//                        result.put("order_amount", after.getDouble("total_amount"));
//                        result.put("create_time", after.getLong("create_time"));
//                    }
//                    return result;
//                });
//        mapDs.print();

    }
}
