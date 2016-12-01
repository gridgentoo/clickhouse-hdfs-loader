package com.kugou.loader.clickhouse;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.cli.MainCliParameterParser;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import com.kugou.loader.clickhouse.mapper.partitioner.HostSequencePartitioner;
import com.kugou.loader.clickhouse.reducer.ClickhouseLoaderReducer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/10/29.
 */
public class ClickhouseHdfsLoader extends Configured implements Tool {

    private static final    Log log = LogFactory.getLog(ClickhouseHdfsLoader.class);
    private static final    Pattern CLICKHOUSE_DISTRIBUTED_ENGINE_CAUSE = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *, *'?([A-Za-z0-9_\\-]+)'? *(, *[a-zA-Z0-9_\\-]+\\(([A-Za-z0-9_\\-]+|)\\))? *\\)$");
    private static final    Pattern urlRegexp = Pattern.compile("^jdbc:clickhouse://([a-zA-Z0-9.-]+|\\[[:.a-fA-F0-9]+\\]):([0-9]+)(?:|/|/([a-zA-Z0-9_]+))$");

    private String          clickhouseClusterName = null;
    private boolean         targetTableIsDistributed = false;
    private String          targetTableDatabase = null;
    private String          targetTableFullName = null;
    private String          targetLocalDatabase = null;
    private String          targetLocalTable    = null;
    private String          targetDistributedTableShardingKey = null;
    private int             targetDistributedTableShardingKeyIndex = ConfigurationOptions.DEFAULT_SHARDING_KEY_INDEX;
    private String          targetLocalDailyTableFullName = null;
    private List<String>    clickhouseClusterHosts = null;

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new ClickhouseHdfsLoader(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration(getConf());
        String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();

        MainCliParameterParser cliParameterParser = new MainCliParameterParser();
        cliParameterParser.cmdLineParser.parseArgument(otherArgs);

        conf.set(ConfigurationKeys.CLI_P_CLICKHOUSE_FORMAT, cliParameterParser.clickhouseFormat);
        conf.set(ConfigurationKeys.CLI_P_CONNECT, cliParameterParser.connect);
        conf.set(ConfigurationKeys.CLI_P_REPACE_CHAR,cliParameterParser.replaceChar);
        conf.set(ConfigurationKeys.CLI_P_DRIVER,cliParameterParser.driver);
        conf.set(ConfigurationKeys.CLI_P_EXPORT_DIR,cliParameterParser.exportDir);
        conf.set(ConfigurationKeys.CLI_P_FIELDS_TERMINATED_BY,cliParameterParser.fieldsTerminatedBy);
        conf.set(ConfigurationKeys.CLI_P_NULL_NON_STRING,cliParameterParser.nullNonString);
        conf.set(ConfigurationKeys.CLI_P_NULL_STRING,cliParameterParser.nullString);
        conf.set(ConfigurationKeys.CLI_P_DT,cliParameterParser.dt);
        conf.set(ConfigurationKeys.CLI_P_BATCH_SIZE,String.valueOf(cliParameterParser.batchSize));
        conf.set(ConfigurationKeys.CLI_P_TABLE,cliParameterParser.table);
        conf.setInt(ConfigurationKeys.CLI_P_MAXTRIES, cliParameterParser.maxTries);
        conf.setInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, cliParameterParser.clickhouseHttpPort);
        conf.setInt(ConfigurationKeys.CLI_P_LOADER_TASK_EXECUTOR, cliParameterParser.loaderTaskExecute);

        // generate temp table name
        String tempTablePrefix = cliParameterParser.table+"_"+
                cliParameterParser.dt.replaceAll("-","")+"_"+
                System.currentTimeMillis()/1000+"_";
        conf.set(ConfigurationKeys.LOADER_TEMP_TABLE_PREFIX, tempTablePrefix);

        // init clickhouse parameters
        initClickhouseParameters(conf, cliParameterParser);

        if(cliParameterParser.daily){
            // create local daily table
            createTargetDailyTable(conf, targetLocalTable, cliParameterParser.mode);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickhouseHdfsLoader.class);
        job.setJobName("Clickhouse HDFS Loader");

        job.setMapperClass(conf.getClassByName(cliParameterParser.mapperClass).asSubclass(Mapper.class));
        // 参数配置InputFormat
        job.setInputFormatClass(conf.getClassByName(cliParameterParser.inputFormat).asSubclass(InputFormat.class));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ClickhouseLoaderReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        if(cliParameterParser.numReduceTasks != -1){
            job.setNumReduceTasks(cliParameterParser.numReduceTasks);
        }else if(targetTableIsDistributed){
            if (CollectionUtils.isNotEmpty(clickhouseClusterHosts))
                job.setNumReduceTasks(clickhouseClusterHosts.size() * cliParameterParser.loaderTaskExecute);
        }else{
            job.setNumReduceTasks(cliParameterParser.loaderTaskExecute);
        }

        job.setPartitionerClass(HostSequencePartitioner.class);

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

        FileInputFormat.addInputPath(job, new Path(conf.get(ConfigurationKeys.CLI_P_EXPORT_DIR)));

        int ret = job.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    /**
     * init clickhouse parameter
     *
     * @param configuration
     * @param parser
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void initClickhouseParameters(Configuration configuration, MainCliParameterParser parser) throws SQLException, ClassNotFoundException {
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(parser.connect);
        targetTableDatabase = extractTargetTableDatabase(parser.connect, parser.table);
        configuration.set(ConfigurationKeys.CL_TARGET_TABLE_DATABASE, targetTableDatabase);
        targetTableFullName = parser.table;
        if(!targetTableFullName.contains(".")){
            targetTableFullName = targetTableDatabase + "." + targetTableFullName;
        }

        log.info("Clickhouse Loader : load data to table["+targetTableFullName+"].");
        configuration.set(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME, targetTableFullName);
        String targetCreateDDL = client.queryCreateTableScript(targetTableFullName);
        Matcher m = CLICKHOUSE_DISTRIBUTED_ENGINE_CAUSE.matcher(targetCreateDDL);
        if (m.find()){
            targetTableIsDistributed = true;
            clickhouseClusterName = m.group(1);
            targetLocalDatabase = m.group(2);
            targetLocalTable = m.group(3);
            targetDistributedTableShardingKey = m.group(5);

            clickhouseClusterHosts = client.queryClusterHosts(clickhouseClusterName);
            log.info("Clickhouse Loader : cluster["+clickhouseClusterName+"] look at "+clickhouseClusterHosts.size()+" hosts.");

            configuration.set(ConfigurationKeys.CL_TARGET_CLUSTER_NAME, clickhouseClusterName);
            configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE, targetLocalDatabase);
            configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_TABLE, targetLocalTable);
            configuration.set(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY, targetDistributedTableShardingKey);

            // sharding_key index
            targetDistributedTableShardingKeyIndex = getClickhouseDistributedShardingKeyIndex(configuration);
            configuration.setInt(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX, targetDistributedTableShardingKeyIndex);
            log.info("Clickhouse Loader : target table["+targetTableFullName+"] is Distributed on ["+clickhouseClusterName+"] by sharding["+targetDistributedTableShardingKey+",index="+targetDistributedTableShardingKeyIndex+"], look at ["+targetLocalDatabase+"."+targetLocalTable+"]");
        }

        configuration.setBoolean(ConfigurationKeys.CL_TARGET_TABLE_IS_DISTRIBUTED, targetTableIsDistributed);
    }

    private String extractTargetTableDatabase(String connectionUrl, String targetTable){
        String database;
        if(null != targetTable && targetTable.contains(".") && !targetTable.endsWith(".")){
            database = targetTable.substring(0, targetTable.indexOf("."));
        }else{
            Matcher m = urlRegexp.matcher(connectionUrl);
            if (m.find()) {
                if (m.group(3) != null) {
                    database = m.group(3);
                } else {
                    database = ConfigurationOptions.DEFAULT_DATABASE;
                }
            } else {
                throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + connectionUrl);
            }
        }
        return database;
    }

    private int getClickhouseDistributedShardingKeyIndex(Configuration configuration) throws SQLException, ClassNotFoundException {
        String key = configuration.get(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY);
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT));
        int ret = -1;
        if (StringUtils.isNotBlank(key)){
            String sql = "describe "+configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME);
            ResultSet resultSet = client.executeQuery(sql);
            int index = 0;
            while(resultSet.next()){
                String line = resultSet.getString(1);
                if (key.equalsIgnoreCase(line.trim())){
                    ret = index;
                    break;
                }else{
                    index ++;
                }
            }
            resultSet.close();
        }
        return ret;
    }

    /**
     * 创建本地日表
     * @param configuration
     * @param targetLocalTable
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void createTargetDailyTable(Configuration configuration, String targetLocalTable, String mode) throws SQLException, ClassNotFoundException {
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT));
        // daily table suffix
        String dailyTableSuffix = "_"+configuration.get(ConfigurationKeys.CLI_P_DT).replaceAll("-","");
        String localDatabase    = (StringUtils.isNotBlank(configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE)) ? configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE) : targetTableDatabase);
        // local daily table name
        String localDailyTableName = targetLocalTable + dailyTableSuffix;

        // local daily table fullname
        targetLocalDailyTableFullName = localDatabase + "." + localDailyTableName;
        // target local target fullname
        String targetLocalTableFullName = localDatabase + "." + targetLocalTable;

        log.info("Clickhouse Loader : query target table["+targetLocalTableFullName+"] DDL");
        String targetLocalTableCreateDDL = client.queryCreateTableScript(targetLocalTableFullName);
        String targetLocalDailyCreateDDL = targetLocalTableCreateDDL.replaceAll(targetLocalTable, localDailyTableName);

        if(targetTableIsDistributed){
            // distributed daily table name
            String targetDistributedDailyTableName      = configuration.get(ConfigurationKeys.CLI_P_TABLE) + dailyTableSuffix;
            String targetDistributedDailyTableFullname  = configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME) + dailyTableSuffix;
            String targetDistributedTableCreateDDL      = client.queryCreateTableScript(configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME));
            String targetDistributedDailyCreateDDL      = targetDistributedTableCreateDDL.replaceAll(configuration.get(ConfigurationKeys.CLI_P_TABLE), targetDistributedDailyTableName);

            for (String host : clickhouseClusterHosts){
                client = ClickhouseClientHolder.getClickhouseClient(host,
                        configuration.getInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, ConfigurationOptions.DEFAULT_CLICKHOUSE_HTTP_PORT),
                        configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE));
                if (client.isTableExists(targetLocalDailyTableFullName)){
                    if (mode.equalsIgnoreCase(ConfigurationOptions.RULE_OF_DROP_DAILY_TABLE)){
                        log.info("Clickhouse Loader : host["+host+"] drop table ["+targetLocalTableFullName+"]");
                        client.dropTableIfExists(targetLocalDailyTableFullName);
                        log.info("Clickhouse Loader : host["+host+"] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
                        client.executeUpdate(targetLocalDailyCreateDDL);
                    }
                }else{
                    log.info("Clickhouse Loader : host["+host+"] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
                    client.executeUpdate(targetLocalDailyCreateDDL);
                }
                if (client.isTableExists(targetDistributedDailyTableFullname)){
                    if (mode.equalsIgnoreCase(ConfigurationOptions.RULE_OF_DROP_DAILY_TABLE)){
                        log.info("Clickhouse Loader : host["+host+"] drop table ["+targetDistributedDailyTableFullname+"]");
                        client.dropTableIfExists(targetDistributedDailyTableFullname);
                        log.info("Clickhouse Loader : host["+host+"] create daily table["+targetDistributedDailyTableFullname+"] ddl["+targetDistributedDailyCreateDDL+"]");
                        client.executeUpdate(targetDistributedDailyCreateDDL);
                    }
                }else{
                    log.info("Clickhouse Loader : host["+host+"] create daily table["+targetDistributedDailyTableFullname+"] ddl["+targetDistributedDailyCreateDDL+"]");
                    client.executeUpdate(targetDistributedDailyCreateDDL);
                }
            }
        }else{
            if (client.isTableExists(targetLocalDailyTableFullName)){
                if (mode.equalsIgnoreCase(ConfigurationOptions.RULE_OF_DROP_DAILY_TABLE)){
                    log.info("Clickhouse Loader : host[localhost] drop table ["+targetLocalTableFullName+"]");
                    client.dropTableIfExists(targetLocalDailyTableFullName);
                    log.info("Clickhouse Loader : host[localhost] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
                    client.executeUpdate(targetLocalDailyCreateDDL);
                }
            }else{
                log.info("Clickhouse Loader : host[localhost] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
                client.executeUpdate(targetLocalDailyCreateDDL);
            }
        }

        configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_DAILY_TABLE_FULLNAME, targetLocalDailyTableFullName);
    }

}
