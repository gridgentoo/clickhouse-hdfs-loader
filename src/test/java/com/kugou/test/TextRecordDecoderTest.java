package com.kugou.test;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.mapper.decode.DefaultRowRecordDecoder;
import com.kugou.loader.clickhouse.mapper.record.decoder.TextRecordDecoder;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Created by jaykelin on 2017/4/6.
 */
public class TextRecordDecoderTest {

    @Test
    public void decoderTest(){
        Configuration conf = new Configuration();
        conf.set(ConfigurationKeys.CLI_P_FIELDS_TERMINATED_BY, "|");
        TextRecordDecoder recordDecoder = new TextRecordDecoder(new ClickhouseConfiguration(conf));

        DefaultRowRecordDecoder rowRecordDecoder = new DefaultRowRecordDecoder(conf, recordDecoder);
        rowRecordDecoder.setRowRecord(null, new Text("xxx|wwf3ef|中文|||wwsdsdf|123123|"));
        while(rowRecordDecoder.hasNext()){
            Tuple.Tuple2 tuple2 = rowRecordDecoder.nextTuple();
            System.out.println(tuple2._1()+":"+tuple2._2());
        }
    }
}
