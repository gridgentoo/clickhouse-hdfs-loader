package com.kugou.loader.clickhouse.mapper.format;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/10.
 */
public class ClickhouseHDFSLoaderOutputCommitter extends FileOutputCommitter {

    private static final Log log = LogFactory.getLog(ClickhouseHDFSLoaderOutputCommitter.class);

    private final static Pattern urlRegexp = Pattern.compile("^jdbc:clickhouse://([a-zA-Z0-9.-]+|\\[[:.a-fA-F0-9]+\\]):([0-9]+)(?:|/|/([a-zA-Z0-9_]+))$");
    private final static String DEFAULT_DATABASE = "default";

    private String tempTable;
    private String database;

    public ClickhouseHDFSLoaderOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return true;
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        log.info("Clickhouse JDBC : OutputCommitter Setup.");
        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
        String url = clickhouseJDBCConfiguration.getConnectUrl();
        Matcher m = urlRegexp.matcher(url);
        if (m.find()) {
            if (m.group(3) != null) {
                this.database = m.group(3);
            } else {
                this.database = DEFAULT_DATABASE;
            }
        } else {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + url);
        }
        String taskid = context.getTaskAttemptID().getTaskID().toString();

        this.tempTable = clickhouseJDBCConfiguration.getTempTablePrefix()+taskid.substring(taskid.indexOf("m_"))+"_"+context.getTaskAttemptID().getId();
        // 初始化临时环境
        initTempEnv(clickhouseJDBCConfiguration);

        super.setupTask(context);
    }

    /**
     * 初始化临时环境
     * @param configuration
     * @throws IOException
     */
    private void initTempEnv(ClickhouseConfiguration configuration) throws IOException{
        Connection conn = null;
        Statement statement = null;
        try {
            String table = configuration.getTableName();
            conn = configuration.getConnection();
            statement = conn.createStatement();
            ResultSet ret = statement.executeQuery("SHOW CREATE TABLE "+table);
            if(ret.next()){
                String createTableDDL = ret.getString(1);
                createTableDDL = createTableDDL.replace(this.database+"."+table, "temp."+this.tempTable);
                createTableDDL = createTableDDL.substring(0, createTableDDL.indexOf("=")+1);
                createTableDDL += " StripeLog";
                // create temp table
                createTempTable(configuration, statement, createTableDDL, 0, null);
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e.getCause());
        } finally {
            try {
                if(null != statement){
                    statement.close();
                }
                if(null != conn){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
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
    private void createTempTable(ClickhouseConfiguration configuration, Statement statement,
                                 String ddl, int tries, Throwable cause) throws IOException{
        try {
            if(tries <= configuration.getMaxTries()){
                statement.executeUpdate(ddl);
            }else{
                throw new IOException("Clickhouse JDBC : create temp table[temp."+this.tempTable+"] failed.", cause);
            }
        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : Create temp table failed. tries : " + tries + " : " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 60000l);
            } catch (InterruptedException e1) {
            }
            createTempTable(configuration, statement, ddl, tries + 1, e.getCause());
        }
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        log.info("Clickhouse JDBC : OutputCommitter Commit.");

        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
        Connection conn = null;
        Statement statement = null;
        try {
            conn = clickhouseJDBCConfiguration.getConnection();
            statement = conn.createStatement();
            String sql = "INSERT INTO "+this.database+"."+clickhouseJDBCConfiguration.getTableName()+" SELECT * FROM temp."+tempTable;

            log.info("Clickhouse JDBC : INSERT DATA FROM StripeLog temp table. max-tries["+clickhouseJDBCConfiguration.getMaxTries()+"], sql["+sql+"]");
            // 插入数据
            insertData(clickhouseJDBCConfiguration, statement, sql, 0, null);

            // 清除临时数据
            sql = "DROP TABLE temp."+tempTable;
            log.info("Clickhouse JDBC : Clean StripeLog temp table. max-tries["+clickhouseJDBCConfiguration.getMaxTries()+"], sql["+sql+"]");
            cleanupTempData(clickhouseJDBCConfiguration, statement, sql, 0, null);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try{
                if(null != statement){
                    statement.close();
                }
                if(null != conn){
                    conn.close();
                }
            }catch(Exception e){
                log.warn(e.getMessage());
            }
        }

        super.commitTask(context);
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        log.info("Clickhouse JDBC : OutputCommitter Abort.");

        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
        Connection conn = null;
        Statement statement = null;
        try {
            conn = clickhouseJDBCConfiguration.getConnection();
            statement = conn.createStatement();

            // 清除临时数据
            String sql = "DROP TABLE temp."+tempTable;
            log.info("Clickhouse JDBC : Clean StripeLog temp table.sql["+sql+"]");
            cleanupTempData(clickhouseJDBCConfiguration, statement, sql, 0, null);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try{
                if(null != statement){
                    statement.close();
                }
                if(null != conn){
                    conn.close();
                }
            }catch(Exception e){
                log.warn(e.getMessage());
            }
        }

        super.abortTask(context);
    }

    private void insertData(ClickhouseConfiguration configuration, Statement statement,
                            String sql, int tries, Throwable cause) throws IOException{
        try{
            if(tries < configuration.getMaxTries()){
                statement.executeUpdate(sql);
            }else{
                throw new IOException("Clickhouse JDBC : Insert data failed. sql["+sql+"]", cause);
            }
        }catch(SQLException e){
            log.warn("Clickhouse JDBC : Insert data failed. tries : "+tries+" : "+e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 60000l);
            } catch (InterruptedException e1) {
            }
            insertData(configuration, statement, sql, tries + 1, e);
        }
    }

    private void cleanupTempData(ClickhouseConfiguration configuration, Statement statement,
                                 String sql, int tries, Throwable cause) throws IOException{
        try{
            if(tries < configuration.getMaxTries()){
                statement.executeUpdate(sql);
            }else{
                throw new IOException("Clickhouse JDBC : Clean temp data failed. sql["+sql+"]", cause);
            }
        }catch(SQLException e){
            log.warn("Clickhouse JDBC : Clean temp data failed. tries : " + tries + " : " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 60000l);
            } catch (InterruptedException e1) {
            }
            insertData(configuration, statement, sql, tries+1, e);
        }
    }
}
