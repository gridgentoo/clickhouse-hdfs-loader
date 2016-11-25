package com.kugou.loader.clickhouse;

import com.kugou.loader.clickhouse.cli.MainCliParameterParser;
import com.kugou.loader.clickhouse.mapper.ClickhouseJDBCConfiguration;
import com.kugou.loader.clickhouse.reducer.ClickhouseLoaderReducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
        conf.setInt(ClickhouseJDBCConfiguration.CLI_P_MAXTRIES, cliParameterParser.maxTries);
        conf.setInt(ClickhouseJDBCConfiguration.CLI_P_CLICKHOUSE_HTTP_PORT, cliParameterParser.clickhouseHttpPort);

        // generate temp table name
        String tempTablePrefix = cliParameterParser.table+"_"+
                cliParameterParser.dt.replaceAll("-","")+"_"+
                System.currentTimeMillis()/1000+"_";
        conf.set(ClickhouseJDBCConfiguration.LOADER_TEMP_TABLE_PREFIX, tempTablePrefix);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickhouseHdfsLoader.class);
        job.setJobName("Clickhouse HDFS Loader");

        job.setMapperClass(conf.getClassByName(cliParameterParser.mapperClass).asSubclass(Mapper.class));
        // 参数配置InputFormat
        job.setInputFormatClass(conf.getClassByName(cliParameterParser.inputFormat).asSubclass(InputFormat.class));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);

        job.setReducerClass(ClickhouseLoaderReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        if(cliParameterParser.numReduceTasks != -1){
            job.setNumReduceTasks(cliParameterParser.numReduceTasks);
        }

        //设置Map关闭推测执行task
        job.setMapSpeculativeExecution(false);
//        if (!conf.getBoolean(ConfigurationOptions.MAPPER_MAP_SPECULATIVE_EXECUTION, true)) {
//            job.setMapSpeculativeExecution(false);
//        }
        //设置Reduce关闭推测执行task
        job.setReduceSpeculativeExecution(false);
//        if (!conf.getBoolean(ConfigurationOptions.REDUCE_MAP_SPECULATIVE_EXECUTION, true)) {
//            job.setReduceSpeculativeExecution(false);
//        }

        FileInputFormat.addInputPath(job, new Path(conf.get(ClickhouseJDBCConfiguration.CLI_P_EXPORT_DIR)));

        int ret = job.waitForCompletion(true) ? 0 : 1;

        return ret;
    }
}
