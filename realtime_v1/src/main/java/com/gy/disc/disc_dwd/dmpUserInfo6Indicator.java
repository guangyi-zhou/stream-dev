package com.gy.disc.disc_dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.bean.TablepenviceOrderBean;
import com.gy.constat.constat;
import com.gy.disc.*;
import com.gy.disc.bean.DimBaseCategory;
import com.gy.realtime_dim.flinkfcation.flinksorceutil;
import com.gy.utils.ConfigUtils;
import com.gy.utils.Hbaseutli;
import com.gy.utils.JdbcUtils;
import com.gy.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Package com.gy.disc.disc_dwd.dwd_user_info
 * @Author guangyi_zhou
 * @Date 2025/5/12 15:09
 * @description: 对用户进行过滤
 */
public class dmpUserInfo6Indicator extends BaseApp {

    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    constat.MYSQL_URL,
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from stream_retail.base_category3 as b3  \n" +
                    "     join stream_retail.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join stream_retail.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public static void main(String[] args) throws Exception {
        new dmpUserInfo6Indicator().start(10051, 1, "dmpUserInfo6Indicator", "dmp_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {


//       kafkaStrDS.print("kafk");
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

//        userInfoSupMsgTableData.print("user_info_sup_msg");
        //创建消费者对象
        KafkaSource<String> source = flinksorceutil.getkafkasorce("dwd_traffic_page");

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

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = PageLogDateStream.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());

//        processStagePageLogDs.print("page去重后");
        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        win2MinutesPageLogsDs.print("page的数据合并数组");
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        mapDeviceAndSearchRateResultDs.print();

        //布隆过滤器userInfo数据
        SingleOutputStreamOperator<JSONObject> BloomfilterSUserInfoDs = UserInfoTableData.keyBy(jsonObj -> jsonObj.getJSONObject("after").getLong("id"))
                .filter(new FilterBloomDeduplicatorFunc_v2(1000000, 0.01));



        //order_info表
        SingleOutputStreamOperator<JSONObject> userInfoDs = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info".equals(jsonObj.getJSONObject("source").getString("table")))
                .uid("filter kafka user info")
                .name("filter kafka user info");
        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "order_info".equals(jsonObj.getJSONObject("source").getString("table")))
                .uid("filter kafka order info")
                .name("filter kafka order info");

        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "order_detail".equals(jsonObj.getJSONObject("source").getString("table")))
                .uid("filter kafka order detail")
                .name("filter kafka order detail");

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());

        // 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));
//        mapOrderInfoAndDetailModelDs.print();

        SingleOutputStreamOperator<JSONObject> UserInfo = BloomfilterSUserInfoDs.map(jsonObject -> {
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
//        UserInfo.print("UserInfo");
//        对userInfoSupMsgTableData进行处理
//        userInfoSupMsgTableData.print();
        SingleOutputStreamOperator<JSONObject> userInfoSumMsgBean = userInfoSupMsgTableData.
                keyBy(data -> data.getJSONObject("after").getLong("uid")).map(jsonObject -> {
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
//        userInfoSumMsgBean.print("userInfoSumMsgBean");


        // 让 UserInfo 和 userInfoSumMsgBean 进行join
        //关联身高体重
        SingleOutputStreamOperator<JSONObject> userInfoJoinUserSumMsgDs = UserInfo
                .keyBy(o -> o.getInteger("uid"))
                .intervalJoin(userInfoSumMsgBean.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-5), Time.minutes(5))
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
//        {"birthday":"1970-07-07","uname":"魏淑","gender":"home","weight":"63","uid":"740","login_name":"z5e0xq","constellation":"巨蟹座","unit_height":"cm","ear":1970,"user_level":"1","phone_num":"13511947252","unit_weight":"kg","email":"z5e0xq@live.com","ts_ms":1747013022083,"height":"178"}
//
//        userInfoJoinUserSumMsgDs.print("userINfo");
////        win2MinutesPageLogsDs.print("page数据");
//
//
//        userInfoJoinUserSumMsgDs.map(data -> data.toJSONString())
//                .sinkTo(
//                        KafkaUtils.buildKafkaSink("cdh01:9092,cdh02:9092,cdh03:9092","kafka_label_base6_topic")
//                );

        mapOrderInfoAndDetailModelDs.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink("cdh01:9092,cdh02:9092,cdh03:9092","kafka_label_base4_topic")
                );

//        mapDeviceAndSearchRateResultDs.map(data -> data.toJSONString())
//                .sinkTo(
//                        KafkaUtils.buildKafkaSink("cdh01:9092,cdh02:9092,cdh03:9092","kafka_label_base2_topic")
//                );

//        userInfoJoinUserSumMsgDs.print("processIntervalJoinUserInfo6BaseMessageDs: ");
        mapDeviceAndSearchRateResultDs.print("mapDeviceAndSearchRateResultDs: ");
//        mapOrderInfoAndDetailModelDs.print("mapOrderInfoAndDetailModelDs: ");









//        orderInfoSupMsgTableData.print("order_info数据");
//        {"op":"r","after":{"payment_way":"3501","refundable_time":1747678014000,"original_total_amount":"129.00","order_status":"1003","consignee_tel":"13831967956","trade_body":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 等1件商品","id":3683,"operate_time":1747073232000,"consignee":"朱飘育","create_time":1747073214000,"expire_time":1747073814000,"coupon_reduce_amount":"30.00","out_trade_no":"763397614635674","total_amount":"99.00","user_id":729,"province_id":9,"activity_reduce_amount":"0.00"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"stream_retail","table":"order_info"},"ts_ms":1747013011724}

//        // 去重数据
//        KeyedStream<JSONObject, Integer> orderInfoKeyByDS = orderInfoSupMsgTableData.keyBy(o -> o.getJSONObject("after").getInteger("id"));
////        orderInfoKeyByDS.print("orderInfo-->");
//
//        //order_info表
//        SingleOutputStreamOperator<JSONObject> orderDetailSupMsgTableData = kafkaStrDS.map(JSON::parseObject)
//                .filter(jsonObj -> "order_detail".equals(jsonObj.getJSONObject("source").getString("table")));
//        // 去重数据
//        KeyedStream<JSONObject, Integer> orderDetailKeyByDS = orderDetailSupMsgTableData.keyBy(o -> o.getJSONObject("after").getInteger("id"));
////        orderDetailKeyByDS.print("orderDetail-->");
//
//        SingleOutputStreamOperator<JSONObject> orderInfoJoinOrderDetailDs = orderInfoKeyByDS
//                .keyBy(data -> data.getJSONObject("after").getLong("id"))
//                .intervalJoin(orderDetailKeyByDS.keyBy(data -> data.getJSONObject("after").getLong("order_id")))
//                .between(Time.minutes(-3), Time.minutes(3))
//                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
//                    @Override
//                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                        JSONObject result = new JSONObject();
//                        result.put("order_id", j1.getJSONObject("after").getString("id"));
//                        result.put("total_amount", j1.getJSONObject("after").getLong("total_amount"));
//                        result.put("user_id", j1.getJSONObject("after").getString("user_id"));
//                        result.put("create_time", j1.getJSONObject("after").getString("create_time"));
//                        result.put("sku_id", j2.getJSONObject("after").getLong("sku_id"));
//                        collector.collect(result);
//                    }
//                })
//                .uid("orderInfo join orderDetail date")
//                .name("orderInfo join orderDetail date");
////        orderInfoJoinOrderDetailDs.print("orderInfoJoinOrderDetailDs");
//        //关联 sku 取 三级 品类 id 和 品牌 id
//        SingleOutputStreamOperator<JSONObject> order_detail_sku = orderInfoJoinOrderDetailDs.map(new OrderDetailSkuHbase());
//        //关联 三级品类
//        SingleOutputStreamOperator<JSONObject> order_detail_sku_category3 = order_detail_sku.map(new order_detail_sku_category3Func());
////        {"tm_id":"4","category3_name":"平板电视","category3_id":"86","category2_id":"16"}
//        //关联 二级品类
//        SingleOutputStreamOperator<JSONObject> order_detail_sku_category2 = order_detail_sku_category3.map(new order_detail_sku_category2Func());
//        //关联  一级 品类
//        SingleOutputStreamOperator<JSONObject> order_detail_sku_category = order_detail_sku_category2.map(new order_detail_sku_categoryFunc());
//        //关联 品牌表
//        SingleOutputStreamOperator<JSONObject> order_detail_sku_tm = order_detail_sku.map(new order_detail_sku_tm_func());
//
////        order_detail_sku.print();
////        userInfoJoinUserSumMsgDs.print("user");
////        order_detail_sku.print();
////
//        SingleOutputStreamOperator<JSONObject> UserJoinCategory3Ds = userInfoJoinUserSumMsgDs
//                .keyBy(data -> data.getLong("uid"))
//                .intervalJoin(order_detail_sku_category.keyBy(data -> data.getLong("uid")))
//                .between(Time.minutes(-3), Time.minutes(3))
//                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
//                    @Override
//                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                        JSONObject result = new JSONObject();
//                        result.putAll(j1);
//                        result.put("category1_name", j2.getString("category1_name"));
//                        result.put("total_amount",j2.getString("total_amount"));
//                        result.put("tm_id",j2.getString("tm_id"));
//                        result.put("create_time",j2.getString("create_time"));
//                        collector.collect(result);
//                        collector.collect(j1);
//
//                    }
//                })
//                .uid("categoryJoinTmDs")
//                .name("categoryJoinTmDs");
////        UserJoinCategory3Ds.print();
//        /*
//        * {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"category1_name":"手机","user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"category1_name":"手机","user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"category1_name":"手机","user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//          {"birthday":"2006-05-08","uname":"司空柔竹","gender":"F","weight":"48","uid":"34","login_name":"bra8fz","constellation":"金牛座","unit_height":"cm","ear":2000,"category1_name":"手机","user_level":"1","phone_num":"13317542416","unit_weight":"kg","email":"bra8fz@yahoo.com","ts_ms":1747278255279,"height":"149"}
//        *
//        * */
//
//        SingleOutputStreamOperator<JSONObject> BloomUserAndC3Ds = UserJoinCategory3Ds.keyBy(data -> data.getLong("uid"))
//                .filter(new FilterBloomDeduplicatorFunc_v4(10000000, 0.01));
////        {"birthday":"1970-05-01","uname":"诸葛军","gender":"home","weight":"72","uid":"52","login_name":"rerkfk9ku14q","constellation":"金牛座","unit_height":"cm","ear":1970,"category1_name":"家用电器","user_level":"2","phone_num":"13459997759","unit_weight":"kg","email":"rerkfk9ku14q@0355.net","ts_ms":1747278255279,"height":"182"}
//
////        BloomUserAndC3Ds.print();
////        /*
////        * {"category1_id":"8","tm_name":"索芙特","uid":"57","category2_name":"香水彩妆","tm_id":"8","category1_name":"个护化妆","category3_name":"唇部","category3_id":"477","category2_id":"54"}
////        page的数据合并数组> {"uid":"696","os":"iOS,Android","ch":"Appstore,wandoujia","pv":10,"md":"iPhone 14 Plus,vivo x90","search_item":"","ba":"iPhone,vivo"}
////         * */
//        SingleOutputStreamOperator<JSONObject> UserINfoJoinCategoryDs = BloomUserAndC3Ds
//                .keyBy(data -> data.getLong("tm_id"))
//                .intervalJoin(order_detail_sku_tm.keyBy(data -> data.getLong("tm_id")))
//                .between(Time.minutes(-3), Time.minutes(3))
//                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
//                    @Override
//                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                        JSONObject result = new JSONObject();
//                        result.putAll(j1);
//                        result.put("tm_name", j2.getString("tm_name"));
//                        collector.collect(result);
//                    }
//                })
//                .uid("UserINfoJoinCategoryDs")
//                .name("UserINfoJoinCategoryDs");
//
////        UserINfoJoinCategoryDs.print();
//
//        UserINfoJoinCategoryDs.keyBy(data -> data.getLong("uid"))
//                .filter(new FilterBloomDeduplicatorFunc_v4(10000000, 0.01)).print();
//
//        // 设备打分模型
//        SingleOutputStreamOperator<JSONObject> userInfoYearScoringModelDS = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
////        userInfoYearScoringModelDS.print("打分模型");
//        /*
//         * 打分模型> {"device_35_39":0.05,"os":"Android","device_50":0.03,"search_25_29":0,"ch":"wandoujia","pv":4,"device_30_34":0.06,"device_18_24":0.08,"search_50":0,"search_40_49":0,"uid":"812","device_25_29":0.07,"md":"xiaomi 12 ultra ","search_18_24":0,"judge_os":"Android","search_35_39":0,"device_40_49":0.04,"search_item":"","ba":"xiaomi","search_30_34":0}
//         * 打分模型> {"device_35_39":0.05,"os":"Android","device_50":0.03,"search_25_29":0,"ch":"xiaomi","pv":6,"device_30_34":0.06,"device_18_24":0.08,"search_50":0,"search_40_49":0,"uid":"325","device_25_29":0.07,"md":"vivo x90,Redmi k50","search_18_24":0,"judge_os":"Android","search_35_39":0,"device_40_49":0.04,"search_item":"","ba":"vivo,Redmi","search_30_34":0}
//         *
//         * */
////        SingleOutputStreamOperator<String> userInfoJoinUserSumMsgDsString = userInfoJoinUserSumMsgDs.map(data -> data.toJSONString());
////        userInfoJoinUserSumMsgDsString.sinkTo(finksink.getkafkasink("dmp_user_info"));
    }
}
