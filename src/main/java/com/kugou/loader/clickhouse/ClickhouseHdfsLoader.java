package com.kugou.loader.clickhouse;

import com.kugou.loader.clickhouse.cli.MainCliParameterParser;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
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

        MainCliParameterParser cliParameterParser = new MainCliParameterParser();
        cliParameterParser.cmdLineParser.parseArgument(otherArgs);

        conf.set(ClickhouseJDBCConfiguration.CLI_P_CLICKHOUSE_FORMAT, cliParameterParser.clickhouseFormat);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_CONNECT, cliParameterParser.connect);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_REPACE_CHAR,cliParameterParser.replaceChar);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_DRIVER,cliParameterParser.driver);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_EXPORT_DIR,cliParameterParser.exportDir);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_FIELDS_TERMINATED_BY,cliParameterParser.fieldsTerminatedBy);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_NULL_NON_STRING,cliParameterParser.nullNonString);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_NULL_STRING,cliParameterParser.nullString);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_DT,cliParameterParser.dt);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_BATCH_SIZE,String.valueOf(cliParameterParser.batchSize));
        conf.set(ClickhouseJDBCConfiguration.CLI_P_TABLE,cliParameterParser.table);
        conf.set(ClickhouseJDBCConfiguration.CLI_P_MAXTRIES, String.valueOf(cliParameterParser.maxTries));


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

        //设置Map关闭推测执行task
        if (!conf.getBoolean(ConfigurationOptions.MAPRED_MAP_SPECULATIVE_EXECUTION, true)) {
            job.setMapSpeculativeExecution(false);
        }

        FileInputFormat.addInputPath(job, new Path(conf.get(ClickhouseJDBCConfiguration.CLI_P_EXPORT_DIR)));

        int ret = job.waitForCompletion(true) ? 0 : 1;

        return ret;
    }
}
