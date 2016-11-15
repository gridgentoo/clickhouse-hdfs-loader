package com.kugou.loader.clickhouse.mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class ClickhouseJDBCConfiguration {

    private final static Log log = LogFactory.getLog(ClickhouseJDBCConfiguration.class);

    public final static String CLI_P_CONNECT = "connect";
    public final static String CLI_P_CLICKHOUSE_FORMAT="clickhouse-format";
    public final static String CLI_P_REPACE_CHAR = "replace-char";
    public final static String CLI_P_DRIVER = "driver";
    public final static String CLI_P_EXPORT_DIR = "export-dir";
    public final static String CLI_P_FIELDS_TERMINATED_BY="fields-terminated-by";
    public final static String CLI_P_NULL_NON_STRING = "null-non-string";
    public final static String CLI_P_NULL_STRING = "null-string";
    public final static String CLI_P_DT = "dt";
    public final static String CLI_P_BATCH_SIZE = "batch-size";
    public final static String CLI_P_TABLE = "table";
    public final static String CLI_P_MAXTRIES = "max-tries";

    public final static String LOADER_TEMP_TABLE_PREFIX = "loader_temp_table_prefix";

    private Configuration conf;

    public ClickhouseJDBCConfiguration(Configuration configuration){
        this.conf = configuration;
    }

    public Connection getConnection() throws ClassNotFoundException, SQLException {
        log.info("Clickhouse : Get JDBC Connection for :"+getConnectUrl());
        Class.forName(getDriver());
        return DriverManager.getConnection(getConnectUrl());
    }

    public String getTableName(){
        return conf.get(CLI_P_TABLE);
    }

    public String getDriver(){
        return conf.get(CLI_P_DRIVER);
    }

    public String getConnectUrl(){
        return conf.get(CLI_P_CONNECT);
    }

    public String getClickhouseFormat(){
        return conf.get(CLI_P_CLICKHOUSE_FORMAT);
    }

    public String getReplaceChar(){
        return conf.get(CLI_P_REPACE_CHAR);
    }

    public String getDt(){
        return conf.get(CLI_P_DT);
    }

    public String getNullNonString(){
        return conf.get(CLI_P_NULL_NON_STRING);
    }

    public String getNullString(){
        return conf.get(CLI_P_NULL_STRING);
    }

    public String getFieldsTerminatedBy(){
        return conf.get(CLI_P_FIELDS_TERMINATED_BY);
    }

    public int getBatchSize(){
        return conf.getInt(CLI_P_BATCH_SIZE, 100);
    }

    public int getMaxTries(){
        return conf.getInt(CLI_P_MAXTRIES, 3);
    }

    public String getTempTablePrefix(){
        return conf.get(LOADER_TEMP_TABLE_PREFIX);
    }
}
