package com.gy.disc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.gy.disc.pageLogMapFunction
 * @Author guangyi_zhou
 * @Date 2025/5/13 14:56
 * @description:
 */
public class pageLogMapFunction implements MapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        JSONObject common = jsonObject.getJSONObject("common");
        JSONObject page = jsonObject.getJSONObject("page");
        if (jsonObject.containsKey("common")) {
            result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");
            result.put("ch", common.getString("ch"));
            result.put("ar", common.getString("ar"));
            result.put("md", common.getString("md"));
            result.put("vc", common.getString("vc"));
            result.put("ba", common.getString("ba"));
            result.put("page_id", page.getString("page_id"));
            result.put("during_type", page.getString("during_type"));
            result.put("item_type", page.getString("item_type"));
            result.put("las_page_id", page.getString("las_page_id"));
            result.put("ts", jsonObject.getLongValue("ts"));
        }
        if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
            JSONObject pageInfo = jsonObject.getJSONObject("page");
            if(pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                String item = pageInfo.getString("item");
                result.put("search_item",item);
            }
        }
        String os = jsonObject.getJSONObject("common").getString("os").split(" ")[0];
        result.put("os",os);
        return result;
    }
}
