package com.kugou.loader.clickhouse.reducer;

import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/15.
 */
public class ClickhouseLoaderReducer extends Reducer<Text, Text, NullWritable, Text>{

    private static final Log log = LogFactory.getLog(ClickhouseLoaderReducer.class);

    private static final Pattern CLICKHOUSE_CLUSTER_ID_PATTERN = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *,");
    private static final Pattern CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *,");

    private boolean         targetIsDistributeTable = false;
    private String          tempDistributedTable = null;
    private String          clickhouseClusterName = null;
    private String          distributedLocalDatabase = null;

    private int maxTries;

    private Connection connection;
    private Statement statement;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());

        this.maxTries = clickhouseJDBCConfiguration.getMaxTries();
        try {
            this.connection = clickhouseJDBCConfiguration.getConnection();
            this.statement = connection.createStatement();

            // init env
            initTempEnv(clickhouseJDBCConfiguration);

            if(targetIsDistributeTable){
                Class.forName(clickhouseJDBCConfiguration.getDriver());
            }
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
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        Iterator<Text> it = values.iterator();
        String targetTable = clickhouseJDBCConfiguration.getDatabase()+"."+clickhouseJDBCConfiguration.getTableName();
        while(it.hasNext()){
            Text value = it.next();
            if(value != null){
                String tempTable = value.toString();

                // insert
                insertFromTemp(clickhouseJDBCConfiguration, key, this.statement, tempTable, targetTable, 0);
            }
        }
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
            while(ret.next()){
                String createTableDDL = ret.getString(1);
                if(null != createTableDDL){
                    Matcher m = CLICKHOUSE_CLUSTER_ID_PATTERN.matcher(createTableDDL);
                    if(m.find()){
                        clickhouseClusterName = m.group(1);
                    }
                    Matcher m2 = CLICKHOUSE_DISTRIBUTED_LOCAL_DATABASE.matcher(createTableDDL);
                    if(m2.find()){
                        distributedLocalDatabase = m2.group(2);
                    }
                    break;
                }
            }
            ret.close();

            // 判断是否是Distributed
            ret = statement.executeQuery("select name from system.tables where database = '"+configuration.getDatabase()+"' and name = '"+ configuration.getTableName()+"' and engine='Distributed'");
            if(ret.next()){
                targetIsDistributeTable = true;
            }
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
     *
     * @param statement
     * @param tempTable
     * @param targetTable
     */
    protected synchronized void insertFromTemp(ClickhouseJDBCConfiguration config, Text key,
                                               Statement statement, String tempTable,
                                               String targetTable, int tries) throws IOException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(targetTable).append(" SELECT * FROM ").append(tempTable);
        if(maxTries <= tries){
            throw new IOException("Clickhouse JDBC : execute sql["+sb.toString()+"] all tries failed.");
        }
        Connection conn = null;
        Statement stat = null;
        try {
            if(targetIsDistributeTable){
                if (null != distributedLocalDatabase){
                    sb = new StringBuilder("INSERT INTO ");
                    sb.append(distributedLocalDatabase).append(".").append(config.getTableName()).append(" ");
                    sb.append("SELECT * FROM ").append(tempTable);
                }
                String connection = "jdbc:clickhouse://"+key.toString()+":"+config.getClickhouseHttpPort();
                Properties properties = new Properties();
                properties.setProperty("profile", "insert");
                conn = DriverManager.getConnection(connection, properties);
                stat = conn.createStatement();

                log.info("Clickhouse JDBC : execute sql["+sb+"]...");
                stat.executeUpdate(sb.toString());
                // drop temp table
                cleanTemp(stat, tempTable, 0);
            }else{
                log.info("Clickhouse JDBC : execute sql["+sb+"]...");
                statement.executeUpdate(sb.toString());
                // drop temp table
                cleanTemp(statement, tempTable, 0);
            }

        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : execute sql[" + sb + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 30*1000l);
            } catch (InterruptedException e1) {
            } finally {
                insertFromTemp(config, key, statement, tempTable, targetTable, tries + 1);
            }
        } finally {
            try{
                if (null != stat){
                    stat.close();
                }
                if (null != conn){
                    conn.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param statement
     * @param tempTable
     * @param tries
     */
    protected synchronized void cleanTemp(Statement statement, String tempTable, int tries) throws IOException {
        StringBuilder sb = new StringBuilder("DROP TABLE ").append(tempTable);
        if(maxTries <= tries){
            throw new IOException("Clickhouse JDBC : execute sql["+sb.toString()+"] all tries failed.");
        }
        log.info("Clickhouse JDBC : execute sql["+sb+"]...");
        try {
            statement.executeUpdate(sb.toString());
        } catch (SQLException e) {
            log.info("Clickhouse JDBC : execute sql[" + sb + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 30*1000l);
            } catch (InterruptedException e1) {
            } finally {
                cleanTemp(statement, tempTable, tries + 1);
            }
        }
    }
}
