package com.kugou.loader.clickhouse.mapper.format;


import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class ClickhouseJDBCOutputFormat<NullWritable, Text> extends OutputFormat<NullWritable, Text> {


    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(taskAttemptContext.getConfiguration());
        String table = clickhouseJDBCConfiguration.getTableName();
        int batchSize = clickhouseJDBCConfiguration.getBatchSize();
        String format = clickhouseJDBCConfiguration.getClickhouseFormat();
        int maxTries = clickhouseJDBCConfiguration.getMaxTries();

        RecordWriter writer = null;
        try {
            writer = new ClickhouseJDBCRecordWriter(clickhouseJDBCConfiguration.getConnection(), format, table, batchSize, maxTries);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e.getCause());
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e.getCause());
        }

        return writer;
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext), taskAttemptContext);
    }

    public class ClickhouseJDBCRecordWriter extends RecordWriter<NullWritable, Text>{

        private final Log log = LogFactory.getLog(ClickhouseJDBCRecordWriter.class);

        private Connection connection;
        private Statement statement;
        private String format;
        private String table;
        private int batchSize;
        private int maxTries;

        private final String sqlHeader;
        private StringBuffer cache = new StringBuffer();
        private int currentIndex = 0;

        private int tries = 0;


        public ClickhouseJDBCRecordWriter(Connection connection,String format, String table,
                                          int batchSize, int maxTries) throws SQLException {
            this.connection = connection;
            this.statement = this.connection.createStatement();
            this.format = format;
            this.table = table;
            this.batchSize = batchSize;
            this.maxTries = maxTries;

            sqlHeader = "INSERT INTO "+this.table +" FORMAT "+this.format;
            log.info("Clickhouse JDBC : sql_header : "+sqlHeader);
        }

        @Override
        public void write(NullWritable nullWritable, Text text) throws IOException, InterruptedException {
            synchronized (this){
                if(currentIndex >= this.batchSize){
                    batchCommit();
                }
                if(cache.length() <= 0){
                    log.debug("Clickhouse JDBC : Insert Sample data like that[" + text.toString() + "]");
                    cache.append(sqlHeader).append("\n");
                }
                cache.append(text.toString()).append("\n");
                currentIndex++;
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(cache.length() > 0){
                batchCommit();
            }
            try {
                this.statement.close();
                this.connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }

        public void batchCommit(){
            try {
                if(this.tries < maxTries){
                    long l = System.currentTimeMillis();
                    log.info("Clickhouse JDBC : batch_commit["+this.tries+"] start. batchsize="+currentIndex);
                    this.statement.executeUpdate(cache.toString());
                    log.info("Clickhouse JDBC : batch_commit["+this.tries+"] end. take time "+(System.currentTimeMillis() - l)+"ms.");
                }else{
                    log.warn("Clickhouse JDBC : "+maxTries+" times tries all failed. batchsize="+currentIndex);
                    // TODO 所有尝试都失败了
                }
                // 初始化所有参数
                this.tries = 0;
                currentIndex = 0;
                // 清空cache
                cache.setLength(0);
            } catch (SQLException e) {
                log.error("Clickhouse JDBC : failed. COUSE BY "+e.getMessage());
                this.tries++;
                try {
                    Thread.sleep(this.tries*3000l);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                this.batchCommit();
            }
        }
    }
}
