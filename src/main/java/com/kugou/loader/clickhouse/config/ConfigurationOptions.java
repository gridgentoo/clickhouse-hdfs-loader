package com.kugou.loader.clickhouse.config;

/**
 * Created by jaykelin on 2016/11/4.
 */
public class ConfigurationOptions {

    public static final String MAPPER_MAP_SPECULATIVE_EXECUTION = "mapreduce.map.speculative" ;//设置Map推测执行task
    public static final String REDUCE_MAP_SPECULATIVE_EXECUTION = "reduce.map.speculative" ;//设置Map推测执行task

    public static final int     DEFAULT_CLICKHOUSE_HTTP_PORT = 8123;
    public static final int     DEFAULT_SHARDING_KEY_INDEX   = -1;
    public static final String  DEFAULT_DATABASE = "default";
    public static final String  DEFAULT_TEMP_DATABASE = "temp";
    public static final String  DEFAULT_CLICKHOUSE_HOST = "localhost";
    public static final int     DEFAULT_LOADER_TASK_EXECUTOR = 1;
    public static final char    DEFAULT_RESULT_FIELD_SPERATOR = '\t';
    public static final String  DEFAULT_RESULT_NULL_STRING = "";
    public static final String  DEFAULT_RESULT_NULL_NON_STRING = "";
    public static final boolean DEFAULT_EXTRACT_HIVE_PARTITIONS = true;

    public static final String  RULE_OF_APPEND_DAILY_TABLE = "append";
    public static final String  RULE_OF_DROP_DAILY_TABLE   = "drop";
}
