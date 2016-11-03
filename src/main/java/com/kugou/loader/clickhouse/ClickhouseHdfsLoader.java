package com.kugou.loader.clickhouse;

import com.kugou.loader.clickhouse.cli.MainCliParamterPraser;
import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import com.kugou.loader.clickhouse.mapper.ClickhouseLoaderMapper;
import com.kugou.loader.clickhouse.mapper.format.ClickhouseJDBCOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapreduce.OrcInputFormat;

/**
 * Created by jaykelin on 2016/10/29.
 */
public class ClickhouseHdfsLoader extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(ClickhouseHdfsLoader.class);

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new ClickhouseHdfsLoader(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration(getConf());
        String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();

        MainCliParamterPraser cliParamterPraser = new MainCliParamterPraser();
        cliParamterPraser.cmdLineParser.parseArgument(otherArgs);

        conf.set(ClickhouseJDBCConfiguration.CLI_P_CLICKHOUSE_FORMAT, cliParamterPraser.clickhouseFormat);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_CONNECT, cliParamterPraser.connect);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_REPACE_CHAR,cliParamterPraser.replaceChar);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_DRIVER,cliParamterPraser.driver);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_EXPORT_DIR,cliParamterPraser.exportDir);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_FIELDS_TERMINATED_BY,cliParamterPraser.fieldsTerminatedBy);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_NULL_NON_STRING,cliParamterPraser.nullNonString);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_NULL_STRING,cliParamterPraser.nullString);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_DT,cliParamterPraser.dt);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_BATCH_SIZE,String.valueOf(cliParamterPraser.batchSize));
        conf.set(ClickhouseJDBCConfiguration.CLI_P_TABLE,cliParamterPraser.table);


        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickhouseHdfsLoader.class);
        job.setJobName("Clickhouse HDFS Loader");
        job.setMapperClass(ClickhouseLoaderMapper.class);

        job.setOutputFormatClass(ClickhouseJDBCOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(OrcInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(conf.get(ClickhouseJDBCConfiguration.CLI_P_EXPORT_DIR)));

        int ret = job.waitForCompletion(true) ? 0 : 1;

        return ret;
    }
}
