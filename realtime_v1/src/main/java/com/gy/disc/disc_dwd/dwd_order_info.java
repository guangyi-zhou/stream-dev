package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.gy.disc.disc_dwd.dwd_oreder_info
 * @Author guangyi_zhou
 * @Date 2025/5/12 21:25
 * @description:
 */
public class dwd_order_info extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dwd_user_info().start(10052,1,"dwd_user_info","dmp_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> orderInfoDS = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "order_info".equals(jsonObj.getJSONObject("source").getString("table")));

        orderInfoDS.print();
    }
}
