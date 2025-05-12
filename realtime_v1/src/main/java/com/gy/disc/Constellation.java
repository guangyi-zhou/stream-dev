package com.gy.disc;

import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * @Package com.gy.disc.Constellation
 * @Author guangyi_zhou
 * @Date 2025/5/12 22:36
 * @description:
 */
public class Constellation {
    public static String getConstellation(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return null;
        }

        try {
            // 解析日期
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            int month = date.getMonthValue();
            int day = date.getDayOfMonth();

            // 判断星座
            if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
                return "水瓶座";
            } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
                return "双鱼座";
            } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
                return "白羊座";
            } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
                return "金牛座";
            } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
                return "双子座";
            } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
                return "巨蟹座";
            } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
                return "狮子座";
            } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
                return "处女座";
            } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
                return "天秤座";
            } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
                return "天蝎座";
            } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
                return "射手座";
            } else {
                return "摩羯座";
            }
        } catch (DateTimeParseException e) {
            // 日期格式错误，返回null或抛出异常
            return null;
        }
    }
}
