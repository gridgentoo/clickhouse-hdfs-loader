package com.kugou.loader.clickhouse.mapper.record.decoder;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.mapper.decode.RecordDecoderConfigurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;

/**
 * Created by jaykelin on 2017/4/5.
 */
public class OrcRecordDecoder extends RecordDecoderConfigurable<NullWritable,OrcStruct> {

    NullWritable key = null;
    OrcStruct    value = null;
    int          currentIndex = 0;
    int          totalFields = -1;

    public OrcRecordDecoder(ClickhouseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean hasNext() {
        return this.currentIndex < this.totalFields;
    }

    @Override
    public void setRecord(NullWritable key, OrcStruct value) {
        this.key = key;
        this.value = value;
        this.currentIndex = 0;
        this.totalFields = value.getNumFields();
    }

    @Override
    public String next() {
        return value.getFieldValue(currentIndex++).toString();
    }
}
