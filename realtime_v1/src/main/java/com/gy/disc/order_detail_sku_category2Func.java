package com.gy.disc;


import com.alibaba.fastjson.JSONObject;
import com.gy.utils.Hbaseutli;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package DWD.func.order_detail_sku_category2Func
 * @Author guangyi_zhou
 * @Date 2025/5/14 15:45
 * @description:  关联 二级品类
 */
public class order_detail_sku_category2Func extends RichMapFunction<JSONObject, JSONObject> {

    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = Hbaseutli.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        Hbaseutli.closeHBaseConnection(hbaseConn);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String skuId = jsonObject.getString("category2_id");
        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "stream_retail", "dim_base_category2", skuId, JSONObject.class);
        JSONObject a = new JSONObject();
        a.putAll(jsonObject);
        a.put("category2_name", skuInfoJsonObj.getString("name"));
        a.put("category1_id", skuInfoJsonObj.getString("category1_id"));

        return a;
    }
}
