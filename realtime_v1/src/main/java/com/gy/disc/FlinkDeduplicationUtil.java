package com.gy.disc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

/**
 * @Package com.gy.disc.MaxTsPerUserFunction
 * @Author guangyi_zhou
 * @Date 2025/5/13 16:46
 * @description:
 */
public class FlinkDeduplicationUtil {
    /**
     * 自定义KeyedProcessFunction实现按用户ID保留最大时间戳记录
     * 每个用户ID对应的记录中，仅保留时间戳字段(ts)值最大的记录
     */
    public static class MaxTsPerUserFunction extends KeyedProcessFunction<Long, JSONObject, JSONObject> {
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
        public void processElement(
                JSONObject jsonObject,
                Context context,
                Collector<JSONObject> collector) throws Exception {

            // 获取当前记录的时间戳
            long currentTs = jsonObject.getLong("ts");

            // 获取当前存储的最大时间戳记录
            JSONObject maxTsRecord = maxTsRecordState.value();

            // 如果是第一条记录或当前记录时间戳更大，则更新状态
            if (maxTsRecord == null || currentTs > maxTsRecord.getLong("ts")) {
                maxTsRecordState.update(jsonObject);
            }
        }
    }
}
