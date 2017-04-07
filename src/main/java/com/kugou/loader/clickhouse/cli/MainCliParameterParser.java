package com.kugou.loader.clickhouse.cli;

import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created by jaykelin on 2016/11/1.
 */
public class MainCliParameterParser {

    public CmdLineParser cmdLineParser;

    @Option(name="--connect", required = true, usage = "jdbc:clickhouse://<host>:<port>/<database>")
    public String connect;

    @Option(name="--driver", required = false, usage = "--driver ru.yandex.clickhouse.ClickHouseDriver")
    public String driver = "ru.yandex.clickhouse.ClickHouseDriver";

    @Option(name="--table", required = true, usage = "导入目标表名")
    public String table;

    @Option(name="--export-dir", required = true, usage = "数据源HDFS路径")
    public String exportDir;

    @Option(name="--fields-terminated-by", required = false, usage = "数据源字段分隔符")
    public String fieldsTerminatedBy = "|";

    @Option(name="--null-non-string", required = false, usage = "")
    public String nullNonString = "";

    @Option(name="--null-string",required = false, usage ="")
    public String nullString = "";

    @Option(name="--clickhouse-format", required = false, usage = "--clickhouse-format [TabSeparated|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TabSeparatedRaw|CSV|CSVWithNames]")
    public String clickhouseFormat = "TabSeparated";

    @Option(name="--replace-char", required = false, usage = "替换在输出字段内容中存在的关键字")
    public String replaceChar = " ";

    @Option(name="--dt",required = true, usage = "导入数据的业务时间,format:YYYY-MM-DD")
    public String dt;

    @Option(name="--batch-size", required = false, usage = "batch size load data into clickhouse")
    public int batchSize = 100000;

    @Option(name="--max-tries", required = false, usage = "当在导入发生异常时的重试次数")
    public int maxTries = 3;

    @Option(name="--num-reduce-tasks", required = false, usage = "指定reduce task的个数，一般不用设置")
    public int numReduceTasks = -1;

    @Option(name="--clickhouse-http-port", required = false, usage = "")
    public int clickhouseHttpPort = 8123;

    @Option(name="-i", required = false, usage = "数据源格式，支持[text|orc]")
    public String i;

    @Option(name="--input-format", required = false, usage = "Deprecated!请使用-i参数替代. ")
    public String inputFormat = "org.apache.orc.mapreduce.OrcInputFormat";

    @Option(name="--mapper-class", required = false, usage = "Deprecated!请使用-i参数替代. ")
    public String mapperClass = "com.kugou.loader.clickhouse.mapper.OrcLoaderMapper";

    @Option(name="--mode", required = false, usage = "<append> 导入时如果日表已存在，数据将会累加.\n<drop> 导入时如果日表已存在，将会重建日表.")
    public String mode = "append";

    @Option(name="--daily", required = false, usage = "是否每天一个独立表")
    public String daily = "false";

    @Option(name="--loader-task-executor", required = false, usage = "")
    public int loaderTaskExecute=1;

    @Option(name="--extract-hive-partitions", required = false, usage = "是否根据数据源路径抽取hive分区，如xxx/dt=2017-01-01/pt=ios，将会抽取出两个分区字段，并按顺序追加到每行数据后面。")
    public String extractHivePartitions = "false";

    @Option(name="--daily-expires", required = false, usage = "导入时将会判断过期日表")
    public int dailyExpires = 3;

    @Option(name="--daily-expires-process", required = false, usage = "过期日表的处理规则：merge:把过期日表数据导入base表后删除日表，drop:把过期日表删除")
    public String dailyExpiresProcess = ConfigurationOptions.DailyExpiresProcess.MERGE.toString();

    @Option(name="--exclude-fields", required = false, usage = "被排除的字段下标，从0开始")
    public String excludeFieldIndexs = null;

    @Option(name="--username", required = false, usage = "")
    public String username = null;

    @Option(name="--password", required = false, usage = "")
    public String password = null;

    @Option(name="--help",required = false)
    public boolean help = false;

    public MainCliParameterParser(){
        this.cmdLineParser = new CmdLineParser(this);
    }
}
