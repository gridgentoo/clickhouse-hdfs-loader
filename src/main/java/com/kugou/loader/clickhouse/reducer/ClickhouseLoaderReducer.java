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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());

        this.maxTries = clickhouseJDBCConfiguration.getMaxTries();
        try {
            this.connection = clickhouseJDBCConfiguration.getConnection();

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
        log.info("Clickhouse JDBC : reduce process key["+key.toString()+"].");
        Connection connection = null;
        Statement statement = null;
        try{
            ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
            String targetTable = clickhouseJDBCConfiguration.getTableName();
            if(targetIsDistributeTable){
                targetTable = distributedLocalDatabase + "." + targetTable;

                String connectionUrl = "jdbc:clickhouse://"+key.toString()+":"+clickhouseJDBCConfiguration.getClickhouseHttpPort();
                Properties properties = new Properties();
                properties.setProperty("profile", "insert");
                connection = DriverManager.getConnection(connectionUrl, properties);
                statement = connection.createStatement();
            }else{
                targetTable = clickhouseJDBCConfiguration.getDatabase()+"."+ targetTable;
                connection = this.connection;
                statement = connection.createStatement();
            }
            Iterator<Text> it = values.iterator();
            while(it.hasNext()){
                Text value = it.next();
                if(value != null){
                    String tempTable = value.toString();
                    log.info("Clickhouse JDBC : process host["+key.toString()+"] temptable["+tempTable+"]");

                    StringBuilder sql = new StringBuilder("INSERT INTO ");
                    sql.append(targetTable).append(" SELECT * FROM ").append(tempTable);

                    String sourceDatabase = null;
                    String sourceTable = null;
                    int docIdex = tempTable.indexOf(".");
                    if(docIdex >= 0){
                        sourceDatabase = tempTable.substring(0, docIdex);
                        sourceTable = tempTable.substring(docIdex+1);
                    }else {
                        sourceDatabase = clickhouseJDBCConfiguration.getDatabase();
                        sourceTable = tempTable;
                    }
                    int count = 0;
                    ResultSet ret = statement.executeQuery("SELECT count(*) FROM system.tables WHERE database = '"+sourceDatabase+"' and name = '"+sourceTable+"'");
                    if(ret.next()){
                        count = ret.getInt(1);
                    }
                    ret.close();

                    if(count == 0){
                        log.warn("Clickhouse JDBC : host["+key.toString()+"] table["+sourceDatabase+"."+sourceTable+"] not exists.");
                        continue;
                    }
                    // insert
                    insertFromTemp(statement, sql.toString(), 0);

                    // drop temp table
                    cleanTemp(statement, tempTable, 0);

                    if(targetIsDistributeTable){
                        String connectionUrl = "jdbc:clickhouse://"+key.toString()+":"+clickhouseJDBCConfiguration.getClickhouseHttpPort();
                        Properties properties = new Properties();
                        properties.setProperty("profile", "insert");
                        connection = DriverManager.getConnection(connectionUrl, properties);
                    }else{
                        connection = this.connection;
                    }
                }
            }
        } catch (SQLException e){
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } finally {
            if (null != connection && targetIsDistributeTable){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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
     * @param insertSQL
     * @param tries
     * @throws IOException
     */
    protected synchronized void insertFromTemp(Statement statement,String insertSQL, int tries) throws IOException {
        if(maxTries <= tries){
            throw new IOException("Clickhouse JDBC : execute sql["+insertSQL+"] all tries failed.");
        }
        try {
            log.info("Clickhouse JDBC : execute sql["+insertSQL+"]...");
            statement.executeUpdate(insertSQL);
        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : execute sql[" + insertSQL + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 30*1000l);
            } catch (InterruptedException e1) {
            } finally {
                insertFromTemp(statement, insertSQL, tries + 1);
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
            log.warn("Clickhouse JDBC : execute sql["+sb.toString()+"] all tries failed.");
            return ;
        }
        try{
            log.info("Clickhouse JDBC : execute sql["+sb+"]...");
            statement.executeUpdate(sb.toString());

        }catch (SQLException e){
            log.info("Clickhouse JDBC : execute sql[" + sb + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) *1000l);
            } catch (InterruptedException e1) {
            } finally {
                cleanTemp(statement, tempTable, tries + 1);
            }
        }
    }
}
