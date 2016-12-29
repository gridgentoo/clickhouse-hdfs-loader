package com.kugou.loader.clickhouse.reducer;

import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by jaykelin on 2016/11/15.
 */
public class ClickhouseLoaderReducer extends Reducer<Text, Text, NullWritable, Text>{

    private static final Log log = LogFactory.getLog(ClickhouseLoaderReducer.class);

    private boolean         targetIsDistributeTable = false;
    private String          tempDistributedTable = null;
    private String          clickhouseClusterName = null;
    private String          distributedLocalDatabase = null;
    private String          targetLocalDailyTable = null;

    private int maxTries;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());

        this.maxTries = clickhouseJDBCConfiguration.getMaxTries();

        // init env
        initTempEnv(clickhouseJDBCConfiguration);

        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String host = key.toString();
        log.info("Clickhouse JDBC : reduce process key["+host+"].");
        int index = host.indexOf("@");
        if (index > -1){
            host = host.substring(index + 1);
        }
        log.info("Clickhouse JDBC : reduce process host["+host+"].");
        ClickhouseClient client = null;
        try{
            ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
            String targetTable = clickhouseJDBCConfiguration.getTableName();
            if(targetIsDistributeTable){
                targetTable = distributedLocalDatabase + "." + targetTable;
                client = ClickhouseClientHolder.getClickhouseClient(host, clickhouseJDBCConfiguration.getClickhouseHttpPort(),distributedLocalDatabase,
                        clickhouseJDBCConfiguration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME),
                        clickhouseJDBCConfiguration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD)
                );
            }else{
                targetTable = clickhouseJDBCConfiguration.getDatabase()+"."+targetTable;
                client = ClickhouseClientHolder.getClickhouseClient(host, clickhouseJDBCConfiguration.getClickhouseHttpPort(), clickhouseJDBCConfiguration.getDatabase(),
                            clickhouseJDBCConfiguration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME),
                            clickhouseJDBCConfiguration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD)
                        );
            }
            //创建日表
            String dailyTable = clickhouseJDBCConfiguration.get(ConfigurationKeys.CL_TARGET_LOCAL_DAILY_TABLE_FULLNAME);
            if (StringUtils.isNotBlank(dailyTable)){
                targetTable = dailyTable;
            }

            Iterator<Text> it = values.iterator();
            while(it.hasNext()){
                Text value = it.next();
                if(value != null){
                    process(client, host, targetTable, value.toString());
                }
            }
        } catch (Exception e){
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } finally {
            if (null != client){
                client.close();
            }
        }
    }

    /**
     * 初始化临时环境
     * @param configuration
     * @throws IOException
     */
    private void initTempEnv(ClickhouseConfiguration configuration) throws IOException{
        clickhouseClusterName = configuration.get(ConfigurationKeys.CL_TARGET_CLUSTER_NAME);
        distributedLocalDatabase = configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE);
        targetIsDistributeTable  = configuration.getBoolean(ConfigurationKeys.CL_TARGET_TABLE_IS_DISTRIBUTED, false);
    }

    /**
     *
     * @param client
     * @param insertSQL
     * @param tries
     * @throws IOException
     */
    protected synchronized void insertFromTemp(ClickhouseClient client,String insertSQL, int tries) throws IOException {
        if(maxTries <= tries){
            throw new IOException("Clickhouse JDBC : execute sql["+insertSQL+"] all tries failed.");
        }
        try {
            log.info("Clickhouse JDBC : execute sql["+insertSQL+"]...");
            client.insert(insertSQL);
        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : execute sql[" + insertSQL + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 30*1000l);
            } catch (InterruptedException e1) {
            } finally {
                insertFromTemp(client, insertSQL, tries + 1);
            }
        }
    }

    /**
     *
     * @param client
     * @param tempTable
     * @param tries
     */
    protected synchronized void cleanTemp(ClickhouseClient client, String tempTable, int tries) throws IOException {
        if(maxTries <= tries){
            log.warn("Clickhouse JDBC : DROP temp table [" + tempTable + "] all tries failed.");
            return ;
        }
        try{
            log.info("Clickhouse JDBC : DROP temp table ["+tempTable+"]...");
            client.dropTableIfExists(tempTable);
        }catch (SQLException e){
            log.info("Clickhouse JDBC : DROP temp table [" + tempTable + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) *1000l);
            } catch (InterruptedException e1) {
            } finally {
                cleanTemp(client, tempTable, tries + 1);
            }
        }
    }

    /**
     * 处理每一条记录
     * @param client
     * @param host
     * @param targetTable
     * @param sourceTable
     * @throws SQLException
     * @throws IOException
     */
    protected void process(ClickhouseClient client, String host, String targetTable,
                           String sourceTable) throws SQLException, IOException {
        log.info("Clickhouse JDBC : process host["+host+"] temp table["+sourceTable+"]");

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(targetTable).append(" SELECT * FROM ").append(sourceTable);
        log.info("Clickhouse JDBC : process execute sql["+sql.toString()+"]");

        String sourceDatabase = null;
        String sourceTableName = null;
        int docIndex = sourceTable.indexOf(".");
        if(docIndex >= 0){
            sourceDatabase = sourceTable.substring(0, docIndex);
            sourceTableName = sourceTable.substring( docIndex+1 );
        }else {
            sourceDatabase = ConfigurationOptions.DEFAULT_TEMP_DATABASE;
            sourceTableName = sourceTable;
        }
        int count = 0;
        ResultSet ret = client.executeQuery("SELECT count(*) FROM system.tables WHERE database = '"+sourceDatabase+"' and name = '"+sourceTableName+"'");
        if(ret.next()){
            count = ret.getInt(1);
        }
        ret.close();

        if(count == 0){
            String msg = "Clickhouse JDBC : host["+host+"] table["+sourceDatabase+"."+sourceTableName+"] not exists.";
            log.warn(msg);
            throw new SQLException(msg);
        }
        // insert
        insertFromTemp(client, sql.toString(), 0);

        // drop temp table
        cleanTemp(client, sourceDatabase+"."+sourceTableName, 0);
    }
}
