package com.kugou.loader.clickhouse;

import com.google.common.collect.Maps;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by jaykelin on 2016/11/25.
 */
public class ClickhouseClientHolder {

    private static Map<String, ClickhouseClient> clientHolders = Maps.newHashMap();

    public static synchronized ClickhouseClient getClickhouseClient(String host, int port, String database) throws SQLException, ClassNotFoundException {
        String key = "jdbc:clickhouse://"+host+":"+port+"/"+database;
        if(!clientHolders.containsKey(key)){
            clientHolders.put(key, new ClickhouseClient(host, port, database));
        }
        return clientHolders.get(key);
    }

    public static synchronized ClickhouseClient getClickhouseClient(String connectionUrl) throws SQLException, ClassNotFoundException {
        if(!clientHolders.containsKey(connectionUrl)){
            clientHolders.put(connectionUrl, new ClickhouseClient(connectionUrl));
        }
        return clientHolders.get(connectionUrl);
    }
}
