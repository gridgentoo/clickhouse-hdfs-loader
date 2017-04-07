# clickhouse-hdfs-loader
loading hdfs data to clickhouse，Support text or orc.
# Options
    --batch-size N                : batch size load data into clickhouse (default:100000)
    --clickhouse-format VAL       : --clickhouse-format [TabSeparated|TabSeparatedW
                                    ithNames|TabSeparatedWithNamesAndTypes|TabSepar
                                    atedRaw|CSV|CSVWithNames] (default:
                                    TabSeparated)
    --connect VAL                 : jdbc:clickhouse://<host>:<port>/<database>
    --daily VAL                   : 是否每天一个独立表 (default: false)
    --daily-expires N             : 导入时将会判断过期日表 (default: 3)
    --daily-expires-process VAL   : 过期日表的处理规则：merge:把过期日表数据导入base表后删除日表，drop:把过期日表删
                                    除 (default: merge)
    --driver VAL                  : --driver ru.yandex.clickhouse.ClickHouseDriver
                                    (default: ru.yandex.clickhouse.ClickHouseDriver
                                    )
    --dt VAL                      : 导入数据的业务时间,format:YYYY-MM-DD
    --exclude-fields VAL          : 被排除的字段下标，从0开始
    --export-dir VAL              : 数据源HDFS路径
    --extract-hive-partitions VAL : 是否根据数据源路径抽取hive分区，如xxx/dt=2017-01-01/pt=ios，将会抽
                                    取出两个分区字段，并按顺序追加到每行数据后面。 (default: false)
    --fields-terminated-by VAL    : 数据源字段分隔符 (default: |)
    --input-format VAL            : Deprecated!请使用-i参数替代.  (default:
                                    org.apache.orc.mapreduce.OrcInputFormat)
    --mapper-class VAL            : Deprecated!请使用-i参数替代.  (default:
                                    com.kugou.loader.clickhouse.mapper.OrcLoaderMap
                                    per)
    --max-tries N                 : 当在导入发生异常时的重试次数 (default: 3)
    --mode VAL                    : <append> 导入时如果日表已存在，数据将会累加.
                                    <drop> 导入时如果日表已存在，将会重建日表. (default: append)
    --num-reduce-tasks N          : 指定reduce task的个数，一般不用设置 (default: -1)
    --replace-char VAL            : 替换在输出字段内容中存在的关键字 (default:  )
    --table VAL                   : 导入目标表名
    -i VAL                        : 数据源格式，支持[text|orc]
