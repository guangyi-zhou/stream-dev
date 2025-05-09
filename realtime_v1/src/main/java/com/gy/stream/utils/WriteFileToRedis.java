package com.gy.stream.utils;

import redis.clients.jedis.Jedis;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
/**
 * @Package com.bg.sensitive_words.untis.WriteFileToRedis
 * @Author guangyi_zhou
 * @Date 2025/5/8 21:27
 * @description: 将文件数据写入Redis
 */
public class WriteFileToRedis {
    public static void main(String[] args) {
        String file_path = "E:\\workspace\\stream-dev\\realtime_v1\\src\\main\\resources\\Identify-sensitive-words.txt";
        String redis_key = "sensitive_words";
        String redis_password = "123456"; // 替换成实际的Redis密码

        try (Jedis jedis = new Jedis("cdh03", 6379)) {
            // 进行认证
            jedis.auth(redis_password);

            try (BufferedReader reader = new BufferedReader(new FileReader(file_path))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    jedis.sadd(redis_key, line);
                    System.out.println("已将敏感词添加到Redis，key为: " + redis_key);
                }
                System.out.println("文件中所有敏感词已添加到Redis");
            } catch (IOException e) {
                System.out.println("错误: 文件未找到");
            }
        } catch (Exception e) {
            System.out.println("发生错误: " + e.getMessage());
        }
    }
}
