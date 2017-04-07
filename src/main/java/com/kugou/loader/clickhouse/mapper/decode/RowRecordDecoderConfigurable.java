package com.kugou.loader.clickhouse.mapper.decode;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by jaykelin on 2017/4/6.
 */
public abstract class RowRecordDecoderConfigurable<K, V> implements RowRecordDecoder<K, V>{

    protected ClickhouseConfiguration config = null;
    private   int                     distributedTableShardingKeyIndex = -1;
    private   int                     cursor = 0;
    private   RecordDecoderConfigurable recordDecoder = null;

    public RowRecordDecoderConfigurable(Configuration configuration, RecordDecoderConfigurable recordDecoder){
        this.recordDecoder = recordDecoder;
        this.config = new ClickhouseConfiguration(configuration);
        this.recordDecoder.setConfiguration(config);
        this.distributedTableShardingKeyIndex = config.getInt(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX, ConfigurationOptions.DEFAULT_SHARDING_KEY_INDEX);
    }

    @Override
    public void setRowRecord(K key, V value) {
        this.cursor = 0;
        this.recordDecoder.setRecord(key, value);
    }

    @Override
    public boolean hasNext() {
        return null != recordDecoder && recordDecoder.hasNext();
    }

    @Override
    public Tuple.Tuple2<Integer, String> nextTuple() {
        return Tuple.tuple(cursor++, recordDecoder.next());
    }

    public void setConfiguration(ClickhouseConfiguration configuration){
        this.config = configuration;
    }

    /**
     * 当前字段是否是sharding key
     * @return
     */
    public boolean isDistributedTableShardingKey(){
        return cursor == distributedTableShardingKeyIndex;
    }

}
