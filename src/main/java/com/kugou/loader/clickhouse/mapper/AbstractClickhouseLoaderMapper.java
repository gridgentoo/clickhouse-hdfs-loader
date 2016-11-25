package com.kugou.loader.clickhouse.mapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/24.
 */
public abstract class AbstractClickhouseLoaderMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

    private static final Log log = LogFactory.getLog(AbstractClickhouseLoaderMapper.class);

    private static final Pattern CLICKHOUSE_CLUSTER_ID_PATTERN = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *,");
    private static final Pattern CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *,");

    private static final String  DEFAULT_CLICKHOUSE_HOST = "localhost";
    private static final String  DEFAULT_CLICKHOUSE_TEMP_DATABASE = "temp";

    protected int               maxTries;
    protected int               batchSize;
    protected Connection        connection;
    protected Statement         statement;

    private String              sqlHeader;                                          // INSERT INTO <tempTable or tempDistributedTable> FORMET <CSV|Tabxxx>
    private String              tempTable;                                          // temp.tableA_timestamp_m_\d{6}_\d
    private String              tempDistributedTable = null;                        // temp.tableA_timestamp_m_\d{6}_\d_distributed
    private String              clickhouseClusterName = null;                       // in clickhouse config <remove_server>
    private String              distributedLocalDatabase = "default";
    private List<String>        clickhouseClusterHostList = Lists.newArrayList();
    private boolean             targetIsDistributeTable = false;
    private Map<String, String> sqlResultCache = Maps.newHashMap();

    private StringBuffer        records = new StringBuffer();
    private int                 index = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        log.info("Clickhouse JDBC : Mapper Setup.");
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        this.maxTries = clickhouseJDBCConfiguration.getMaxTries();
        this.batchSize = clickhouseJDBCConfiguration.getBatchSize();
        try {
            this.connection = clickhouseJDBCConfiguration.getConnection();
            this.statement = this.connection.createStatement();
            this.tempTable = getTempTableName(context);

            // 初始化参数
            initTempEnv(clickhouseJDBCConfiguration);

            // 创建
            String createTableDDL = createTempTableDDL(clickhouseJDBCConfiguration, tempTable);
            createTempTable(clickhouseJDBCConfiguration, statement, createTableDDL, 0, null);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        }
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        log.info("Clickhouse JDBC : Mapper cleanup.");
        if(records.length() > 0){
            batchInsert(0);
        }
        try{
            if(targetIsDistributeTable){
                // drop distributed
                log.info("Clickhouse JDBC : drop temp distributed table["+tempDistributedTable+"]");
                statement.executeUpdate("DROP TABLE temp."+tempDistributedTable);
            }
        } catch (SQLException e){
            log.warn(e.getMessage(), e);
        } finally {
            try{
                if(null != statement){
                    this.statement.close();
                }
                if(null != connection){
                    this.connection.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        if(targetIsDistributeTable){
            for(String host : clickhouseClusterHostList){
                write(host, tempTable, DEFAULT_CLICKHOUSE_TEMP_DATABASE, context);
            }
        }else{
            write(DEFAULT_CLICKHOUSE_HOST, tempTable, DEFAULT_CLICKHOUSE_TEMP_DATABASE, context);
        }

        super.cleanup(context);
    }

    @Override
    protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        String line = readLine(key, value, context);
        write(line);
    }

    public abstract String readLine(KEYIN key, VALUEIN value, Context context);

    public abstract void write(String host, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException;

    /**
     * 目标输出是否使用Distributed Table
     * @return
     */
    protected boolean targetOutputIsDistributedTable(){
        return targetIsDistributeTable;
    }

    /**
     * Clickhouse Cluster Name
     *
     * @return
     */
    protected String getDistributedClusterName(){
        return clickhouseClusterName;
    }

    protected List<String> getClickhouseClusterHostList(){
        return clickhouseClusterHostList;
    }

    /**
     * 输出
     * @param record
     */
    protected synchronized void write(String record){
        if(index == 0){
            records.append(sqlHeader).append("\n");
        }
        records.append(record).append("\n");
        index ++;
        if(index >= batchSize){
            batchInsert(0);
        }
    }

    protected void batchInsert(int tries) {
        Statement statement = null;
        try {
            if(tries <= maxTries){
                long l = System.currentTimeMillis();
                log.info("Clickhouse JDBC : batch_commit["+tries+"] start. batchsize="+index);
                statement = this.connection.createStatement();
                statement.executeUpdate(records.toString());
                log.info("Clickhouse JDBC : batch_commit["+tries+"] end. take time "+(System.currentTimeMillis() - l)+"ms.");
            }else{
                log.error("Clickhouse JDBC : " + maxTries + " times tries all failed. batchsize=" + index);
                // TODO 所有尝试都失败了
            }
            index = 0;
            records.setLength(0);
        } catch (SQLException e) {
            log.error("Clickhouse JDBC : failed. COUSE BY "+e.getMessage(), e);
            try {
                Thread.sleep((tries+1)*10000l);
            } catch (InterruptedException e1) {
            }
            batchInsert(tries + 1);
        } finally {
            if (null != statement){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 获取临时表名
     * @param context
     * @return
     */
    protected String getTempTableName(Context context){
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        String taskId = context.getTaskAttemptID().getTaskID().toString();
        return clickhouseJDBCConfiguration.getTempTablePrefix()+taskId.substring(taskId.indexOf("m_"))+"_"+context.getTaskAttemptID().getId();
    }

    /**
     * 获取Distributed临时表名
     * @param context
     * @return
     */
    protected String getTempDistributedTableTableName(Context context){
        if(StringUtils.isBlank(tempTable)){
            ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
            String taskId = context.getTaskAttemptID().getTaskID().toString();
            tempTable = clickhouseJDBCConfiguration.getTempTablePrefix()+taskId.substring(taskId.indexOf("m_"))+"_"+context.getTaskAttemptID().getId();
        }

        return tempTable+"_distributed";
    }

    /**
     * 初始化临时环境
     * @param configuration
     * @throws IOException
     */
    private void initTempEnv(ClickhouseJDBCConfiguration configuration) throws IOException{
        try {
            String targetTableName = configuration.getTableName();
            if (!targetTableName.contains(".")){
                targetTableName = configuration.getDatabase()+"."+targetTableName;
            }
            String targetTableDDL = queryTableDDL(targetTableName);

            // 全局参数初始化
            Matcher m = CLICKHOUSE_CLUSTER_ID_PATTERN.matcher(targetTableDDL);
            if(m.find()){
                this.clickhouseClusterName = m.group(1);
            }
            Matcher m2 = CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE.matcher(targetTableDDL);
            if(m2.find()){
                this.distributedLocalDatabase = m2.group(2);
            }
            if(StringUtils.isNotBlank(clickhouseClusterName)){
                this.targetIsDistributeTable = true;
                ResultSet ret = statement.executeQuery("select distinct host_address from system.clusters where cluster='"+this.clickhouseClusterName+"'");
                while(ret.next()){
                    String host = ret.getString(1);
                    if(StringUtils.isNotBlank(host)){
                        log.debug("Clickhouse JDBC : clickhouse cluster["+clickhouseClusterName+"] found host["+host+"]");
                        clickhouseClusterHostList.add(host);
                    }
                }
                ret.close();
            }
            this.tempDistributedTable = tempTable+"_distributed";

            if(this.targetIsDistributeTable){
                this.sqlHeader = "INSERT INTO temp."+ this.tempDistributedTable +" FORMAT "+configuration.getClickhouseFormat();
            }else{
                this.sqlHeader = "INSERT INTO temp."+ this.tempTable +" FORMAT "+configuration.getClickhouseFormat();
            }
            log.info("Clickhouse JDBC : INSERT USING header["+sqlHeader+"]");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e.getCause());
        }
    }

    /**
     * tempTable ddl
     *
     * @param configuration
     * @param tempTableName
     * @return
     */
    protected String createTempTableDDL(ClickhouseJDBCConfiguration configuration, String tempTableName) throws SQLException {
        String targetTableName = configuration.getTableName();
        if (!targetTableName.contains(".")){
            targetTableName = configuration.getDatabase()+"."+targetTableName;
        }
        return createTempTableDDL(targetTableName, tempTableName);
    }

    /**
     * tempTable ddl
     *
     * @param targetTableName
     * @param tempTableName
     * @return
     */
    protected String createTempTableDDL(String targetTableName, String tempTableName) throws SQLException {
        if (!tempTableName.contains(".")) {
            tempTableName = DEFAULT_CLICKHOUSE_TEMP_DATABASE + "." + tempTableName;
        }

        String ddl = queryTableDDL(targetTableName);
        ddl = ddl.replace(targetTableName.toLowerCase(), tempTableName);
        ddl = ddl.substring(0, ddl.indexOf("=") + 1);
        ddl += " StripeLog";
        return ddl;
    }

    /**
     * show create table
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    protected String queryTableDDL(String tableName) throws SQLException {
        String showCreateTableSql = "SHOW CREATE TABLE "+tableName;
        String ddl = null;
        if (!sqlResultCache.containsKey(showCreateTableSql)){
            Statement statement = null;
            try {
                statement = this.connection.createStatement();
                log.info("Clickhouse JDBC : execute sql["+showCreateTableSql+"].");
                ResultSet ret = statement.executeQuery(showCreateTableSql);
                if(ret.next()){
                    ddl = ret.getString(1);
                }else{
                    throw new IllegalArgumentException("Cannot found target table["+tableName+"] create DDL.");
                }
                ret.close();
            } finally {
                if (null != statement){
                    statement.close();
                }
            }
            sqlResultCache.put(showCreateTableSql, ddl);
        }else{
            ddl = sqlResultCache.get(showCreateTableSql);
        }
        return ddl;
    }

    /**
     * 创建临时表
     * @param configuration
     * @param statement
     * @param ddl
     * @param tries
     * @param cause
     * @throws IOException
     */
    private void createTempTable(ClickhouseJDBCConfiguration configuration, Statement statement,
                                 String ddl, int tries, Throwable cause) throws IOException, ClassNotFoundException {
        log.info("Clickhouse JDBC : create temp table["+ddl+"]");
        try {
            if(null == ddl){
                throw new IllegalArgumentException("Clickhouse JDBC : create table dll cannot be null.");
            }
            if(tries <= configuration.getMaxTries()){
                if(this.targetIsDistributeTable){
                    Class.forName(configuration.getDriver());
                    for(String hostAddress : getClickhouseClusterHostList()){
                        Connection connection = null;
                        Statement stat = null;
                        try {
                            try {
                                String connect = "jdbc:clickhouse://" + hostAddress + ":" + configuration.getClickhouseHttpPort();
                                connection = DriverManager.getConnection(connect);
                                stat = connection.createStatement();
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                throw new IOException("Clickhouse JDBC : Distributed create temp table failed.", e);
                            }
                            log.info("Clickhouse JDBC : server["+hostAddress+"], create table table["+ddl+"]");
                            stat.executeUpdate(ddl);
                        } finally {
                            if (null != stat){
                                stat.close();
                            }
                            if (null != connection){
                                connection.close();
                            }
                        }
                    }

                    // 创建 TEMP Distributed table
                    log.debug("Clickhouse JDBC : create temp distributed table["+tempDistributedTable+"].");
                    String distributedTableDDL = ddl.replace(tempTable, tempDistributedTable);
                    String targetTableName = configuration.getTableName();
                    if (!targetTableName.contains(".")){
                        targetTableName = configuration.getDatabase()+"."+targetTableName;
                    }
                    String targetTableDDL = queryTableDDL(targetTableName);
                    String distributedTableEngine = "Distributed("+this.clickhouseClusterName+",temp,"+tempTable+", rand())";
                    if(targetTableDDL.contains("Distributed")){
                        distributedTableEngine = targetTableDDL.substring(targetTableDDL.indexOf("=")+1);
                        distributedTableEngine = distributedTableEngine.replace(distributedLocalDatabase, "temp");
                        distributedTableEngine = distributedTableEngine.replace(configuration.getTableName(), tempTable);
                    }
                    distributedTableDDL = distributedTableDDL.substring(0, distributedTableDDL.indexOf("=")+1);
                    distributedTableDDL = distributedTableDDL + distributedTableEngine;
                    log.info("Clickhouse JDBC : create temp distributed table["+distributedTableDDL+"]");
                    statement.executeUpdate(distributedTableDDL);
                }else{
                    statement.executeUpdate(ddl);
                }
            }else{
                throw new IOException("Clickhouse JDBC : create temp table[temp."+this.tempTable+"] failed.", cause);
            }
        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : Create temp table failed. tries : "+tries+" : "+e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 1000l);
            } catch (InterruptedException e1) {
            }
            createTempTable(configuration, statement, ddl, tries + 1, e.getCause());
        }
    }
}
