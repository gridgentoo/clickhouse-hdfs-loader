package com.kugou.loader.clickhouse.mapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.HostRecordsCache;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import com.kugou.loader.clickhouse.mapper.decode.RowRecordDecoder;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/24.
 */
public abstract class AbstractClickhouseLoaderMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

    private static final Log log = LogFactory.getLog(AbstractClickhouseLoaderMapper.class);

    private static final Pattern HIVE_PARTITIONS_PATTERN = Pattern.compile("([0-9a-zA-Z]+)=([0-9a-zA-Z_\\-]+)/?");
    private static final String  CLICKHOUSE_COUNTERS_GROUP = "Clickhouse Loader Counters";

    protected int               maxTries;
    protected int               batchSize;
    protected String            clickhouseDistributedTableShardingKeyValue = null;
    protected int               clickhouseDistributedTableShardingKeyIndex = -1;
    protected String            sqlHeader;                                          // INSERT INTO <tempTable or tempDistributedTable> FORMET <CSV|Tabxxx>
    protected Map<String, HostRecordsCache> hostRecords = Maps.newHashMap();
    protected Map<String, String> hivePartitions = Maps.newLinkedHashMap();

    private String              tempTable;                                          // temp.tableA_timestamp_m_\d{6}_\d
    private String              tempDistributedTable = null;                        // temp.tableA_timestamp_m_\d{6}_\d_distributed
    private String              clickhouseClusterName = null;                       // in clickhouse config <remove_server>
    private String              distributedLocalDatabase = "default";
    private String              distributedLocalTable = null;
    private List<ClusterNodes>        clickhouseClusterHostList = Lists.newArrayList();
    private boolean             targetIsDistributeTable = true;
    private Map<String, String> sqlResultCache = Maps.newHashMap();
    private String              clickhouseDistributedTableShardingKey = null;
    private HashFunction        hashFn = Hashing.murmur3_128();
//    private List<Integer>       excludeFieldIndexs = Lists.newArrayList();
    private ClickhouseConfiguration config;
    private RowRecordDecoder<KEYIN, VALUEIN> rowRecordDecoder = null;

    private int                 clusterShardTotalWeight = 0;
    private String              mapTaskIdentify = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        log.info("Clickhouse JDBC : Mapper Setup.");
        config = new ClickhouseConfiguration(context.getConfiguration());
        this.maxTries = config.getMaxTries();
        this.batchSize = config.getBatchSize();
        log.info("Clickhouse Loader : batchSize = "+this.batchSize+", maxTries = "+this.maxTries);
        try {
            this.tempTable = getTempTableName(context);

            // 初始化环境
            initTempEnv(context, config);

        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        }
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        log.info("Clickhouse JDBC : Mapper cleanup.");
        ClickhouseConfiguration config = new ClickhouseConfiguration(context.getConfiguration());
        for(String cacheKey : hostRecords.keySet()){
            HostRecordsCache cache = hostRecords.get(cacheKey);
            if(cache.recordsCount > 0){
                cache.ready = true;
                for (ClusterNodes nodes : clickhouseClusterHostList){
                    if (cacheKey.equals(nodes.getKey())){
                        try {
                            batchInsert(context, nodes, config.getClickhouseHttpPort(), cache, this.maxTries);
                        } catch (JSONException e) {
                            log.error(e.getMessage(), e);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        } finally {
                            break;
                        }
                    }
                }
            }
        }

//        int numReduceTask = config.getInt(ConfigurationKeys.CL_NUM_REDUCE_TASK, ConfigurationOptions.DEFAULT_LOADER_TASK_EXECUTOR);
//        int partition = (hashFn.hashString(tempTable).asInt() & Integer.MAX_VALUE) % numReduceTask;
        for(int i = 0; i < clickhouseClusterHostList.size(); i++){
//            write(clickhouseClusterHostList.get(i), i+"."+partition, tempTable, ConfigurationOptions.DEFAULT_TEMP_DATABASE, context);
            write(clickhouseClusterHostList.get(i), this.mapTaskIdentify, tempTable, ConfigurationOptions.DEFAULT_TEMP_DATABASE, context);
        }

        super.cleanup(context);
    }

    @Override
    protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        try{
            rowRecordDecoder.setRowRecord(key, value);
            write(key, readRowRecord(rowRecordDecoder, context), context);
        }catch(IllegalFormatException e){
            log.error(e.getMessage(), e);
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Illegal format records").increment(1);
        } catch (JSONException e) {
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Illegal format records").increment(1);
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Failed records").increment(1);
            log.error(e.getMessage(), e);
        }

    }

    public abstract RowRecordDecoder<KEYIN, VALUEIN> getRowRecordDecoder(Configuration config);

    public abstract void write(ClusterNodes nodes, String mapTaskIdentify, String tempTable, String tempDatabase, Context context)
            throws IOException, InterruptedException;

    /**
     * 读取每一行
     * @param rowRecordDecoder
     * @param context
     * @return
     * @throws IllegalArgumentException
     */
    public String readRowRecord(RowRecordDecoder rowRecordDecoder, Context context) throws IllegalFormatException{
        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
        String nullNonString = clickhouseJDBCConfiguration.getNullNonString();
        String nullString = clickhouseJDBCConfiguration.getNullString();
        String replaceChar = clickhouseJDBCConfiguration.getReplaceChar();
        StringBuilder row = new StringBuilder();
        ConfigurationOptions.ClickhouseFormats clickhouseFormat = ConfigurationOptions.ClickhouseFormats.valueOf(clickhouseJDBCConfiguration.getClickhouseFormat());

        int maxColumnIndex = 0;
        while(rowRecordDecoder.hasNext()){
            Tuple.Tuple2<Integer, String> tuple2 = rowRecordDecoder.nextTuple();
            if (null == tuple2)
                continue;

            maxColumnIndex = tuple2._1();

            if(rowRecordDecoder.isDistributedTableShardingKey()){
                clickhouseDistributedTableShardingKeyValue = tuple2._2();
            }
//            if (getExcludeFieldIndexs().contains(tuple2._1())){
//                continue;
//            }
            if(tuple2._1() != 0 && row.length() > 0) {
                row.append(clickhouseFormat.SPERATOR);
            }
            String field;
            if (null == tuple2._2()){
                field = nullString;
            }
            else if (StringUtils.equals(tuple2._2(), "\\N")){
                field = nullNonString;
            }
            else {
                field = tuple2._2().replace(clickhouseFormat.SPERATOR, replaceChar).replace('\\', '/');
            }
            row.append(field);
//            System.out.println("index="+tuple2._1()+":"+tuple2._2()+":"+field);
        }

        int dataColumnSize = maxColumnIndex + 1;
//        for (int index :excludeFieldIndexs){
//            if (index > maxColumnIndex){
//                continue;
//            }else{
//                dataColumnSize --;
//            }
//        }
        int targetTableColumnSize = clickhouseJDBCConfiguration.getTargetTableColumnSize();

        boolean extractHivePartitions = clickhouseJDBCConfiguration.getBoolean(ConfigurationKeys.CLI_P_EXTRACT_HIVE_PARTITIONS, ConfigurationOptions.DEFAULT_EXTRACT_HIVE_PARTITIONS);
        boolean hasAdditionalColumns = StringUtils.isNotBlank(config.get(ConfigurationKeys.CLI_P_ADDITIONAL_COLUMNS));

        int finalDataColumnSize = dataColumnSize;
        if (extractHivePartitions){
            int position = 0;
            finalDataColumnSize += hivePartitions.size();
            Iterator<Map.Entry<String, String>> it = hivePartitions.entrySet().iterator();
            while (it.hasNext()){
                String column_val = it.next().getValue();
                if (clickhouseDistributedTableShardingKeyIndex == dataColumnSize + position){
                    clickhouseDistributedTableShardingKeyValue = column_val;
                }
                position ++;
                row.append(clickhouseFormat.SPERATOR).append(column_val);
            }
        }
        if (hasAdditionalColumns){
            dataColumnSize = finalDataColumnSize;
            int position = 0;
            StringTokenizer tokenizer = new StringTokenizer(config.get(ConfigurationKeys.CLI_P_ADDITIONAL_COLUMNS), ",");
            while(tokenizer.hasMoreTokens()){
                String column_val = tokenizer.nextToken();
                if (clickhouseDistributedTableShardingKeyIndex == dataColumnSize + position){
                    clickhouseDistributedTableShardingKeyValue = column_val;
                }
                position ++;
                row.append(clickhouseFormat.SPERATOR).append(column_val);
                finalDataColumnSize += 1;
            }
//            String[] addCols = config.get(ConfigurationKeys.CLI_P_ADDITIONAL_COLUMNS).split(",");
//            finalDataColumnSize += addCols.length;
//            for (String addCol : addCols){
//                if (clickhouseDistributedTableShardingKeyIndex == dataColumnSize + position){
//                    clickhouseDistributedTableShardingKeyValue = addCol;
//                }
//                position ++;
//                row.append(clickhouseFormat.SPERATOR).append(addCol);
//            }
        }

        if (finalDataColumnSize != targetTableColumnSize){
            throw new IllegalArgumentException("Target table column size = " + targetTableColumnSize + ", but found record column size = "
                    + finalDataColumnSize +"(data_columnSize["+dataColumnSize+"],extract_hivePartitions["+hivePartitions.size()+"],additional_columnSize["+(finalDataColumnSize-dataColumnSize-hivePartitions.size())+"])");
        }

        return row.toString();
    }


    protected List<ClusterNodes> getClickhouseClusterHostList(){
        return clickhouseClusterHostList;
    }


    private ClusterNodes getClusterNodesByShardIndex(int index){
        int cursor = 0;
        for (int i = 0; i < clickhouseClusterHostList.size(); i++){
            if(index < (cursor += clickhouseClusterHostList.get(i).getShardWeight())){
                return clickhouseClusterHostList.get(i);
            }
        }
        throw new IllegalStateException("Cannot found cluster node by shard_index "+index);
    }

    /**
     * 输出
     * @param record
     */
    protected synchronized void write(KEYIN key, String record, Context context) throws Exception {
        ClickhouseConfiguration configuration = new ClickhouseConfiguration(context.getConfiguration());
        ClusterNodes nodes = null;
        if(getClickhouseClusterHostList().size() > 1){
            int code;

            if(StringUtils.isNotBlank(clickhouseDistributedTableShardingKeyValue)){
                code = hashFn.hashString(clickhouseDistributedTableShardingKeyValue).asInt() & Integer.MAX_VALUE;
            }else{
                code = hashFn.hashString(UUID.randomUUID().toString()).asInt() & Integer.MAX_VALUE;
            }
            // 整体权重按shard_weight划分
            int shardIndex = code % clusterShardTotalWeight;
//            nodes = clickhouseClusterHostList.get(hostIndex);
            nodes = getClusterNodesByShardIndex(shardIndex);
        }else if (getClickhouseClusterHostList().size() == 1){
            nodes = getClickhouseClusterHostList().get(0);
        }
        HostRecordsCache cache = hostRecords.get(nodes.getKey());
        if(cache.recordsCount == 0){
            cache.records.append(sqlHeader).append("\n");
        }
        cache.records.append(record).append("\n");
        cache.recordsCount ++;
        if(cache.recordsCount >= batchSize / getClickhouseClusterHostList().size()){
            cache.ready = true;
            batchInsert(context, nodes, configuration.getClickhouseHttpPort(), cache, this.maxTries);
        }
    }


    /**
     *
     * @param context
     * @param nodes
     * @param port
     * @param cache
     * @param tries
     * @throws JSONException
     */
    protected void batchInsert(Context context, ClusterNodes nodes, int port, HostRecordsCache cache, int tries) throws JSONException, Exception {
        boolean done = false;
        int count = 0;
        Map<String, Boolean> hostStatus = Maps.newHashMap();
        while(!done && count < tries){
            count ++;
            log.info("Clickhouse Loader : try ["+count+"] loading data to cluster -> "+nodes.toString());
            done = true;
            for (int i = 0; i < nodes.getHostsCount(); i++){
                String host = nodes.hostAddress(i);
                boolean status = hostStatus.containsKey(host) ? hostStatus.get(host) : false;
                if (cache.ready && !status){
                    log.info("Clickhouse Loader : try ["+count +"] loading data to host -> " + host + ", batchsize -> "+cache.recordsCount);
                    try {
                        long l = System.currentTimeMillis();
                        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(host, port, ConfigurationOptions.DEFAULT_TEMP_DATABASE, config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
                        client.insert(cache.records.toString());
                        status = true;
                        hostStatus.put(host, status);
                        log.info("Clickhouse Loader : loaded data to host -> " + host +", take time "+(System.currentTimeMillis() - l)+"ms.");
                    } catch (Exception e) {
                        status = false;
                        log.error("Clickhouse JDBC : failed. COUSE BY "+e.getMessage(), e);
                        try {
                            Thread.sleep((tries+1)*10000l);
                        } catch (InterruptedException e1) {
                        }
                    } finally {
                        done &= status;
                    }
                }
            }
        }

        if (!done){
            throw new Exception("Clickhouse JDBC: temp data insert failed. total records = "+cache.recordsCount);
        }else{
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Success records").increment(cache.recordsCount);
        }

        synchronized (cache){
            cache.reset();
        }
    }

    /**
     * 获取临时表名
     * @param context
     * @return
     */
    protected String getTempTableName(Context context){
        ClickhouseConfiguration config = new ClickhouseConfiguration(context.getConfiguration());
        String taskId = context.getTaskAttemptID().getTaskID().toString();
        if (taskId.indexOf("m_") > -1){
            this.mapTaskIdentify = taskId.substring(taskId.indexOf("m_"))+"_"+context.getTaskAttemptID().getId();
        }else{
            throw new IllegalArgumentException("taskid["+taskId+"] cannot found 'm_'");
        }

        return config.getTempTablePrefix()+this.mapTaskIdentify;
    }

    /**
     * 初始化临时环境
     * @param configuration
     * @throws IOException
     */
    private void initTempEnv(Context context, ClickhouseConfiguration configuration) throws SQLException, ClassNotFoundException, IOException, JSONException {
        clickhouseClusterName = configuration.get(ConfigurationKeys.CL_TARGET_CLUSTER_NAME);
        distributedLocalDatabase = configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE);
        distributedLocalTable = configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_TABLE);
        clickhouseDistributedTableShardingKey = configuration.get(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY);

        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(
                configuration.getConnectUrl(),
                configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME),
                configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD)
        );

        // 查询字段总数
        int targetTableColumnSize = 0;
        ResultSet ret1 = client.executeQuery("select count(*) as total_col_size from system.columns where database = '"+this.config.getDatabase()+"' and table = '"+this.config.getTableName()+"'");
        while (ret1.next()){
            targetTableColumnSize = ret1.getInt("total_col_size");
        }
        ret1.close();
        log.info("Clickhouse Loader : Found target table["+this.config.getDatabase()+"."+this.config.getTableName()+"] column size = "+targetTableColumnSize);
        this.config.getConf().setInt(ConfigurationKeys.CL_TARGET_TABLE_COLUMN_SIZE, targetTableColumnSize);

        clickhouseClusterHostList = client.queryClusterHosts(clickhouseClusterName);
        for (ClusterNodes n : clickhouseClusterHostList){
            this.clusterShardTotalWeight += n.getShardWeight();
        }
//        if(StringUtils.isNotBlank(clickhouseClusterName)){
//            this.targetIsDistributeTable = true;
//            ResultSet ret = client.executeQuery("select distinct host_address from system.clusters where cluster='"+this.clickhouseClusterName+"' and replica_num = 1");
//            while(ret.next()){
//                String host = ret.getString(1);
//                if(StringUtils.isNotBlank(host)){
//                    log.debug("Clickhouse Loader : clickhouse cluster["+clickhouseClusterName+"] found host["+host+"]");
//                    clickhouseClusterHostList.add(host);
//                }
//            }
//            ret.close();
//        }
        this.tempDistributedTable = tempTable+"_distributed";

        if (CollectionUtils.isEmpty(clickhouseClusterHostList)){
            targetIsDistributeTable = false;
            clickhouseClusterHostList.add(new ClusterNodes(configuration.extractHostFromConnectionUrl()));
        }

//        String excludeFieldIndexsParameter = configuration.get(ConfigurationKeys.CLI_P_EXCLUDE_FIELD_INDEXS);
//        if(StringUtils.isNotBlank(excludeFieldIndexsParameter)){
//            log.info("Clickhouse Loader : exclude src field column indexs :"+excludeFieldIndexsParameter);
//            Pattern p = Pattern.compile("(\\d+)");
//            Matcher m = p.matcher(excludeFieldIndexsParameter);
//            while(m.find()){
//                excludeFieldIndexs.add(Integer.valueOf(m.group(1)));
//            }
//        }

        // Not for insert into temp distributed table
        this.sqlHeader = "INSERT INTO temp."+ this.tempTable +" FORMAT "+configuration.getClickhouseFormat();
        log.info("Clickhouse JDBC : INSERT USING header["+sqlHeader+"]");

        // 初始化rowRecordDocoder
        this.rowRecordDecoder = getRowRecordDecoder(configuration.getConf());

        // 抽取hive partitions
        if (config.isExtractHivePartitions()){
            hivePartitions = extractHivePartitions(config);
        }

        // 识别 Distributed table Sharding key
        clickhouseDistributedTableShardingKeyIndex = config.getInt(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX, ConfigurationOptions.DEFAULT_SHARDING_KEY_INDEX);
        log.info("Clickhouse JDBC : distributed table sharding key["+clickhouseDistributedTableShardingKey+"] index["+clickhouseDistributedTableShardingKeyIndex+"]");

        // create temp table for all host
        String createTableDDL = createTempTableDDL(config, tempTable);
        for (ClusterNodes host : clickhouseClusterHostList){
            hostRecords.put(host.getKey(), new HostRecordsCache());
            for (int i = 0; i< host.getHostsCount(); i++){
                context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Created temp tables").increment(1);
                createTempTable(config, host.hostAddress(i), createTableDDL, 0, null);
            }

        }

        context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Target table columns").setValue(config.getTargetTableColumnSize());
        context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Distributed local hosts").setValue(clickhouseClusterHostList.size());
    }

    /**
     * tempTable ddl
     *
     * @param configuration
     * @param tempTableName
     * @return
     */
    protected String createTempTableDDL(ClickhouseConfiguration configuration, String tempTableName) throws SQLException {
        String targetTableName = configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME);
        return createTempTableDDL(targetTableName, tempTableName);
    }

    /**
     * tempTable ddl
     *
     * @param targetTableName
     * @param tempTableName
     * @return
     */
    protected String createTempTableDDL(String targetTableName, String tempTableName) throws SQLException {
        if (!tempTableName.contains(".")) {
            tempTableName = ConfigurationOptions.DEFAULT_TEMP_DATABASE + "." + tempTableName;
        }

        String ddl = queryTableDDL(targetTableName);
        ddl = ddl.replace(targetTableName.toLowerCase(), tempTableName);
        ddl = ddl.substring(0, ddl.indexOf("=") + 1);
        ddl += " StripeLog";
        return ddl;
    }

    /**
     * show create table
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    protected String queryTableDDL(String tableName) throws SQLException {
        String showCreateTableSql = "SHOW CREATE TABLE "+tableName;
        String ddl = null;
        if (!sqlResultCache.containsKey(showCreateTableSql)){
            Statement statement = null;
            try {
                ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(
                        config.getConnectUrl(),
                        config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME),
                        config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD)
                );
                log.info("Clickhouse JDBC : execute sql["+showCreateTableSql+"].");
                ResultSet ret = client.executeQuery(showCreateTableSql);
                if(ret.next()){
                    ddl = ret.getString(1);
                }else{
                    throw new IllegalArgumentException("Cannot found target table["+tableName+"] create DDL.");
                }
                ret.close();
            } finally {
                if (null != statement){
                    statement.close();
                }
            }
            sqlResultCache.put(showCreateTableSql, ddl);
        }else{
            ddl = sqlResultCache.get(showCreateTableSql);
        }
        return ddl;
    }

    protected void createTempTable(ClickhouseConfiguration config, String host, String ddl, int tries, Throwable cause) throws IOException, SQLException, ClassNotFoundException {
        log.info("Clickhouse JDBC : create temp table["+ddl+"] for host["+host+"]");
        if(null == ddl){
            throw new IllegalArgumentException("Clickhouse JDBC : create table dll cannot be null.");
        }
        if (tries >= config.getMaxTries()){
            throw new IOException("Clickhouse JDBC : create temp table[temp."+this.tempTable+"] for host["+host+"] failed.", cause);
        }
        try{
            ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(host, config.getClickhouseHttpPort(), ConfigurationOptions.DEFAULT_TEMP_DATABASE, config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), config.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
            client.executeUpdate(ddl);
        }catch (SQLException e){
            log.warn("Clickhouse JDBC : Create temp table for host["+host+"] failed. tries : "+tries+" : "+e.getMessage(), e);
            try {
                Thread.sleep((tries+1) * 1000l);
            } catch (InterruptedException e1) {
            }
            createTempTable(config, host, ddl, tries+1, e);
        }

    }

    /**
     * 从路径抽取hive partition
     * @param configuration
     * @return
     */
    protected Map<String, String> extractHivePartitions(ClickhouseConfiguration configuration){
        String inputPath = configuration.get(ConfigurationKeys.CLI_P_EXPORT_DIR);
        if(StringUtils.isBlank(inputPath)){
            throw new IllegalArgumentException("Clickhouse JDBC : OMS! export-dir can't by empty!");
        }
        Map<String, String> hivePartitions = Maps.newLinkedHashMap();
        Matcher m = HIVE_PARTITIONS_PATTERN.matcher(inputPath);
        while(m.find()){
            if(m.groupCount() >= 2){
                String key = m.group(1);
                String value = m.group(2);
                if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)){
                    log.info("Clickhouse Loader ： Found hive partition ["+key+"="+value+"]");
                    hivePartitions.put(key, value);
                }
            }
        }
        return hivePartitions;
    }

//    protected List<Integer> getExcludeFieldIndexs(){
//        return excludeFieldIndexs;
//    }


}
