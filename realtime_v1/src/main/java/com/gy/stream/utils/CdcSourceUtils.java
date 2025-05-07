package com.gy.stream.utils;


import com.gy.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

/**
 * @Package com.stream.common.utils.CdcSourceUtils
 * @Author zhou.han
 * @Date 2024/12/17 11:49
 * @description: MySQL Cdc Source
 */
public class CdcSourceUtils {

    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,StartupOptions model){
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        return  MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                .serverTimeZone(ConfigUtils.getString("mysql.timezone"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}
