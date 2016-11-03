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

        RecordWriter writer = null;
        try {
            writer = new ClickhouseJDBCRecordWriter(clickhouseJDBCConfiguration.getConnection(), format, table, batchSize);
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

        private final String sqlHeader;
        private StringBuffer cache = new StringBuffer();
        private int currentIndex = 0;


        public ClickhouseJDBCRecordWriter(Connection connection,String format, String table, int batchSize) throws SQLException {
            this.connection = connection;
            this.statement = this.connection.createStatement();
            this.format = format;
            this.table = table;
            this.batchSize = batchSize;

            sqlHeader = "INSERT INTO "+this.table +"FORMAT "+this.format +"\n";
        }

        @Override
        public void write(NullWritable nullWritable, Text text) throws IOException, InterruptedException {
            synchronized (this){
                if(currentIndex > this.batchSize){
                    batchCommit();
                }
                if(cache.length() <= 0){
                    cache.append(sqlHeader);
                }
                cache.append(text.toString()).append("\n");
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
                this.statement.executeUpdate(cache.toString());
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                // TODO 二分插入，减少抛弃数量
            } finally {
                // 清空cache
                cache.setLength(0);
            }
        }
    }
}
