package com.kugou.loader.clickhouse.mapper;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/1.
 */
public class ClickhouseLoaderMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {

    private static final Log log = LogFactory.getLog(ClickhouseLoaderMapper.class);

    private static final Pattern CLICKHOUSE_CLUSTER_ID_PATTERN = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *,");
    private static final Pattern CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *,");

    private static final String  DEFAULT_CLICKHOUSE_HOST = "localhost";

    private int             maxTries;
    private int             batchSize;
    private Connection      connection;
    private Statement       statement;

    private String          sqlHeader;
    private String          tempTable;
    private boolean         targetIsDistributeTable = false;
    private String          tempDistributedTable = null;
    private String          clickhouseClusterName = null;
    private String          distributedLocalDatabase = "default";
    private List<String>    clickhouseClusterHostList = Lists.newArrayList();

    private StringBuffer    records = new StringBuffer();
    private int             index = 0;

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

            initTempEnv(clickhouseJDBCConfiguration);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        }

        if(this.targetIsDistributeTable){
            this.sqlHeader = "INSERT INTO "+ this.tempDistributedTable +" FORMAT "+clickhouseJDBCConfiguration.getClickhouseFormat();
        }else{
            this.sqlHeader = "INSERT INTO "+ this.tempTable +" FORMAT "+clickhouseJDBCConfiguration.getClickhouseFormat();
        }
        log.info("Clickhouse JDBC : insert use header["+sqlHeader+"]");
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
                log.info("Clickhouse JDBC : drop temp distributed table["+this.tempDistributedTable+"]");
                statement.executeUpdate("DROP TABLE "+this.tempDistributedTable);
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
                context.write(new Text(host), new Text(this.tempTable));
            }
        }else{
            context.write(new Text(DEFAULT_CLICKHOUSE_HOST), new Text(this.tempTable));
        }

        super.cleanup(context);
    }

    @Override
    protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        String nullNonString = "";
        String nullString = "";
        String replaceChar = clickhouseJDBCConfiguration.getReplaceChar();
        String dt = clickhouseJDBCConfiguration.getDt();
        StringBuilder row = new StringBuilder();
        for(int i = 0; i < value.getNumFields(); i++){
            if(i != 0) {
                row.append('\t');
            }
            WritableComparable fieldVaule = value.getFieldValue(i);
            String field;
            if(null == fieldVaule){
                field = nullString;
            }else{
                field = fieldVaule.toString();
                if(field.equals("\\N")){
                    field = nullNonString;
                }else if(field.equalsIgnoreCase("NULL")) {
                    field = nullString;
                }else {
                    field = field.replace('\t', replaceChar.charAt(0));
                    field = field.replace('\\', '/');
                }
            }
            row.append(field);
        }
        row.append('\t').append(dt);

        insert(row.toString());

//        context.write(NullWritable.get(), new Text(this.tempTable));
    }

    protected synchronized void insert(String record){
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
        try {
            if(tries <= maxTries){
                long l = System.currentTimeMillis();
                log.info("Clickhouse JDBC : batch_commit["+tries+"] start. batchsize="+batchSize);
                this.statement.executeUpdate(records.toString());
                log.info("Clickhouse JDBC : batch_commit["+tries+"] end. take time "+(System.currentTimeMillis() - l)+"ms.");
            }else{
                log.error("Clickhouse JDBC : " + maxTries + " times tries all failed. batchsize=" + batchSize);
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
        }
    }

    protected String getTempTableName(Context context){
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        String taskId = context.getTaskAttemptID().getTaskID().toString();
        return "temp."+clickhouseJDBCConfiguration.getTempTablePrefix()+taskId.substring(taskId.indexOf("m_"))+"_"+context.getTaskAttemptID().getId();
    }

    /**
     * 初始化临时环境
     * @param configuration
     * @throws IOException
     */
    private void initTempEnv(ClickhouseJDBCConfiguration configuration) throws IOException{
        Statement statement = null;
        try {
            String table = configuration.getTableName();
            table = configuration.getDatabase()+"."+table;
            String sql = "show create table "+table;
            log.info("Clickhouse JDBC : query create table ddl["+sql+"]");
            statement = this.connection.createStatement();
            ResultSet ret = statement.executeQuery(sql);
            String createTableDDL = null;
            while(ret.next()){
                createTableDDL = ret.getString(1);
                if(null != createTableDDL){
                    Matcher m = CLICKHOUSE_CLUSTER_ID_PATTERN.matcher(createTableDDL);
                    if(m.find()){
                        clickhouseClusterName = m.group(1);
                    }
                    Matcher m2 = CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE.matcher(createTableDDL);
                    if(m2.find()){
                        distributedLocalDatabase = m2.group(2);
                    }
                    createTableDDL = createTableDDL.replace(table.toLowerCase(), this.tempTable);
                    createTableDDL = createTableDDL.substring(0, createTableDDL.indexOf("=")+1);
                    createTableDDL += " StripeLog";
                    break;
                }
            }
            ret.close();

            // 判断是否是Distributed
            ret = statement.executeQuery("select name from system.tables where database = '"+configuration.getDatabase()+"' and name = '"+ configuration.getTableName()+"' and engine='Distributed'");
            if(ret.next()){
                targetIsDistributeTable = true;
            }
            // create temp table
            createTempTable(configuration, statement, createTableDDL, 0, null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e.getCause());
        } finally {
            if(null != statement){
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
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
                    ResultSet ret = statement.executeQuery("select distinct host_address from system.clusters where cluster='"+this.clickhouseClusterName+"'");
                    while(ret.next()){
                        String hostAdress = ret.getString(1);
                        if(null != hostAdress){
                            log.debug("Clickhouse JDBC : Found remote server["+hostAdress+"].");
                            clickhouseClusterHostList.add(hostAdress);
                            Connection connection = null;
                            Statement stat = null;
                            try {
                                try {
                                    String connect = "jdbc:clickhouse://" + hostAdress + ":" + configuration.getClickhouseHttpPort();
                                    connection = DriverManager.getConnection(connect);
                                    stat = connection.createStatement();
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                    throw new IOException("Clickhouse JDBC : Distributed create temp table failed.", e);
                                }
                                log.info("Clickhouse JDBC : server["+hostAdress+"], create table table["+ddl+"]");
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
                    }
                    ret.close();

                    // 创建 TEMP Distributed table
                    this.tempDistributedTable = this.tempTable+"_distributed";
                    log.debug("Clickhouse JDBC : create temp distributed table["+this.tempDistributedTable+"].");
                    String distributedTableDDL = ddl.replace(this.tempTable, tempDistributedTable);
                    ret = statement.executeQuery("show create table "+configuration.getDatabase()+"."+configuration.getTableName());
                    String distributedTableEngine = "Distributed("+this.clickhouseClusterName+",temp,"+this.tempTable.substring(tempTable.indexOf(".")+1)+", rand())";
                    if(ret.next()){
                        String targetTableDDL = ret.getString(1);
                        if(targetTableDDL.contains("Distributed")){
                            distributedTableEngine = targetTableDDL.substring(targetTableDDL.indexOf("=")+1);
                            distributedTableEngine = distributedTableEngine.replace(distributedLocalDatabase, "temp");
                            distributedTableEngine = distributedTableEngine.replace(configuration.getTableName(), this.tempTable.substring(tempTable.indexOf(".")+1));
                        }
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
