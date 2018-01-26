package com.kugou.loader.clickhouse.context;

import com.google.common.collect.Maps;
import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.utils.Tuple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by jaykelin on 2018/1/26.
 */
public class ClickhouseLoaderContext {

    private ClickhouseConfiguration configuration = null;
    private ClickhouseClient client = null;
    private Map<Integer, Tuple.Tuple2<String,String>> colDataType = Maps.newHashMap();
    private Integer columnSize = -1;

    public ClickhouseLoaderContext(ClickhouseConfiguration configuration) throws SQLException {
        this.configuration = configuration;
        this.client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
        String sql = "desc "+ getTargetLocalDatabase()+"."+getTargetLocalTable();
        ResultSet resultSet = client.executeQuery(sql);
        int index = -1;
        while(resultSet.next()){
            index ++;
            colDataType.put(index, Tuple.tuple(resultSet.getString(0), resultSet.getString(1)));
        }
        columnSize = index+1;
    }

    public ClickhouseConfiguration getConfiguration(){
        return configuration;
    }

    /**
     * return Distributed Database
     * @return
     */
    public String getTargetDistributedDatabase(){
        return configuration.get(ConfigurationKeys.CL_TARGET_TABLE_DATABASE);
    }

    /**
     * return Distributed table
     * @return
     */
    public String getTargetDistributedTable(){
        return configuration.get(ConfigurationKeys.CLI_P_TABLE);
    }

    public String getTargetLocalDatabase(){
        return configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE);
    }

    public String getTargetLocalTable(){
        return configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE);
    }

    public int getColumnSize(){
        return columnSize;
    }

    public boolean columnIsStringType(int col){
        if (col >= getColumnSize()){
            throw new IllegalArgumentException(String.format("column index[s%] >= columnsize[s%]", col, getColumnSize()));
        }
        Tuple.Tuple2 tuple = colDataType.get(col);
        if (tuple._2().equals("String")){
            return true;
        }
        else {
            return false;
        }
    }
}
