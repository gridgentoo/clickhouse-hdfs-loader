package com.kugou.loader.clickhouse.reducer;

import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * Created by jaykelin on 2016/11/15.
 */
public class ClickhouseLoaderReducer extends Reducer<NullWritable, Text, NullWritable, Text>{

    private static final Log log = LogFactory.getLog(ClickhouseLoaderReducer.class);

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
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        String targetTable = clickhouseJDBCConfiguration.getDatabase()+"."+clickhouseJDBCConfiguration.getTableName();
        while(it.hasNext()){
            Text value = it.next();
            if(value != null){
                String tempTable = value.toString();

                // insert
                insertFromTemp(this.statement, tempTable, targetTable, 0);
            }
        }
    }

    /**
     *
     * @param statement
     * @param tempTable
     * @param targetTable
     */
    protected synchronized void insertFromTemp(Statement statement, String tempTable, String targetTable, int tries) throws IOException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(targetTable).append(" SELECT * FROM ").append(tempTable);
        if(maxTries <= tries){
            throw new IOException("Clickhouse JDBC : execute sql["+sb.toString()+"] all tries failed.");
        }
        log.info("Clickhouse JDBC : execute sql["+sb+"]...");
        try {
            statement.executeUpdate(sb.toString());

            // drop temp table
            cleanTemp(this.statement, tempTable, 0);
        } catch (SQLException e) {
            log.warn("Clickhouse JDBC : execute sql[" + sb + "]...failed, tries["+tries+"]. Cause By " + e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 30*1000l);
            } catch (InterruptedException e1) {
            } finally {
                insertFromTemp(statement, tempTable, targetTable, tries + 1);
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
