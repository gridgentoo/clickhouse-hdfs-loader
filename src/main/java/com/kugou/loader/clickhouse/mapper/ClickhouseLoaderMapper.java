package com.kugou.loader.clickhouse.mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jaykelin on 2016/11/1.
 */
public class ClickhouseLoaderMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {

    private static final Log log = LogFactory.getLog(ClickhouseLoaderMapper.class);

    private int maxTries;
    private int batchSize;
    private Connection connection;
    private Statement statement;

    private String sqlHeader;
    private String tempTable;
    private StringBuffer records = new StringBuffer();
    private int index = 0;

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

        this.sqlHeader = "INSERT INTO "+ this.tempTable +" FORMAT "+clickhouseJDBCConfiguration.getClickhouseFormat();
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        log.info("Clickhouse JDBC : Mapper cleanup.");
        if(records.length() > 0){
            batchInsert(0);
        }
        try{
            if(null != statement){
                this.statement.close();
            }
            if(null != connection){
                this.connection.close();
            }
        } catch (SQLException e){
            log.warn(e.getMessage(), e);
        }

        context.write(NullWritable.get(), new Text(this.tempTable));

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
                    createTableDDL = createTableDDL.replace(table.toLowerCase(), this.tempTable);
                    createTableDDL = createTableDDL.substring(0, createTableDDL.indexOf("=")+1);
                    createTableDDL += " StripeLog";
                    break;
                }
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
                                 String ddl, int tries, Throwable cause) throws IOException{
        log.info("Clickhouse JDBC : create temp table["+ddl+"]");
        try {
            if(null == ddl){
                throw new IllegalArgumentException("Clickhouse JDBC : create table dll cannot be null.");
            }
            if(tries <= configuration.getMaxTries()){
                statement.executeUpdate(ddl);
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
