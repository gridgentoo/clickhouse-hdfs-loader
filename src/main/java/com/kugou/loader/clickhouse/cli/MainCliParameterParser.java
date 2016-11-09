package com.kugou.loader.clickhouse.cli;

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

    @Option(name="--table", required = true, usage = "--table table1")
    public String table;

    @Option(name="--export-dir", required = true, usage = "--export-dir /path")
    public String exportDir;

    @Option(name="--fields-terminated-by", required = false, usage = "--fields-terminated-by '|'")
    public String fieldsTerminatedBy = "|";

    @Option(name="--null-non-string", required = false, usage = "--null-non-string ''")
    public String nullNonString = "";

    @Option(name="--null-string",required = false, usage ="--null-string")
    public String nullString = "";

    @Option(name="--clickhouse-format", required = false, usage = "--clickhouse-format [TabSeparated|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TabSeparatedRaw|BlockTabSeparated|CSV|CSVWithNames|JSON|JSONCompact|JSONEachRow|XML]")
    public String clickhouseFormat = "TabSeparated";

    @Option(name="--replace-char", required = false)
    public String replaceChar = " ";

    @Option(name="--dt",required = true, usage = "--dt 2016-01-01")
    public String dt;

    @Option(name="--batch-size", required = false, usage = "--batch-size 100000")
    public int batchSize = 100;

    @Option(name="--max-tries", required = false, usage = "--max-tries 3")
    public int maxTries = 3;

    public MainCliParameterParser(){
        this.cmdLineParser = new CmdLineParser(this);
    }
}
