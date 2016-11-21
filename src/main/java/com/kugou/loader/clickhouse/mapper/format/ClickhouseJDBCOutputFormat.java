package com.kugou.loader.clickhouse.mapper.format;


import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class ClickhouseJDBCOutputFormat extends TextOutputFormat<NullWritable, Text> {

    private String tempName;

    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {}

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(conf);
        String taskid = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
        tempName = "temp."+clickhouseJDBCConfiguration.getTempTablePrefix()+taskid.substring(taskid.indexOf("m_"))+"_"+taskAttemptContext.getTaskAttemptID().getId();

        boolean isCompressed = getCompressOutput(taskAttemptContext);
        String keyValueSeparator = conf.get(SEPERATOR, "\t");
        CompressionCodec codec = null;
        String extension = "";
        if(isCompressed) {
            Class file = getOutputCompressorClass(taskAttemptContext, GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }

        Path file1 = this.getDefaultWorkFile(taskAttemptContext, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut;
        if(!isCompressed) {
            fileOut = fs.create(file1, false);
            return new ClickhouseJDBCRecordWriter(fileOut, clickhouseJDBCConfiguration, keyValueSeparator);
        } else {
            fileOut = fs.create(file1, false);
            return new ClickhouseJDBCRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), clickhouseJDBCConfiguration, keyValueSeparator);
        }
    }


    public class ClickhouseJDBCRecordWriter extends RecordWriter<NullWritable, Text>{

        private final Log log = LogFactory.getLog(ClickhouseJDBCRecordWriter.class);

        private Connection connection;
        private Statement statement;
        private String format;
        private String table;
        private int batchSize;
        private int maxTries;

        private final byte[] newline;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        private final String sqlHeader;
        private StringBuffer cache = new StringBuffer();
        private int currentIndex = 0;

        private int tries = 0;

        public ClickhouseJDBCRecordWriter(DataOutputStream output, ClickhouseJDBCConfiguration configuration, String keyValueSeparator){
            this.out = output;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
                this.newline = "\n".getBytes("UTF-8");

                this.connection = configuration.getConnection();
                this.statement = this.connection.createStatement();
                this.format = configuration.getClickhouseFormat();
                this.table = tempName;
                this.batchSize = configuration.getBatchSize();
                this.maxTries = configuration.getMaxTries();

                this.sqlHeader = "INSERT INTO "+this.table +" FORMAT "+this.format;

                log.info("Clickhouse JDBC : sql_header : "+sqlHeader);
            } catch (UnsupportedEncodingException var4) {
                throw new IllegalArgumentException("can\'t find UTF-8 encoding");
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("can\'t find class.", e);
            } catch (SQLException e){
                throw new IllegalArgumentException("can\'t get jdbc connection.", e);
            }

        }

        public ClickhouseJDBCRecordWriter(DataOutputStream output, ClickhouseJDBCConfiguration configuration){
            this(output, configuration, "\t");
        }


        private void writeObject(Object o) throws IOException {
            if(o instanceof org.apache.hadoop.io.Text) {
                org.apache.hadoop.io.Text to = (org.apache.hadoop.io.Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes("UTF-8"));
            }

        }

        public synchronized void writeTempTable(NullWritable key, Text value) throws IOException {
            boolean nullKey = key == null || key instanceof org.apache.hadoop.io.NullWritable;
            boolean nullValue = value == null;
            if(!nullKey || !nullValue) {
                if(!nullKey) {
                    this.writeObject(key);
                }

                if(!nullKey && !nullValue) {
                    this.out.write(this.keyValueSeparator);
                }

                if(!nullValue) {
                    this.writeObject(value);
                }

                this.out.write(newline);
            }
        }

        @Override
        public synchronized void write(NullWritable key, Text value) throws IOException, InterruptedException {
            if(currentIndex >= this.batchSize){
                batchCommit();
            }
            if(cache.length() <= 0){
                log.debug("Clickhouse JDBC : Insert Sample data like that[" + value.toString() + "]");
                cache.append(sqlHeader).append("\n");
            }
            cache.append(value.toString()).append("\n");
            currentIndex++;
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(cache.length() > 0){
                batchCommit();
            }
            this.writeTempTable(NullWritable.get(), new Text(tempName));
            this.out.close();
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
                    Thread.sleep(this.tries*10000l);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                this.batchCommit();
            }
        }

    }
}
