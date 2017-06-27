package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.mapper.decode.*;
import com.kugou.loader.clickhouse.mapper.record.decoder.TextRecordDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by jaykelin on 2016/11/29.
 */
public class TextLoaderMapper extends AbstractClickhouseLoaderMapper<Object, Text, Text, Text> {

    private static final Log logger = LogFactory.getLog(TextLoaderMapper.class);

    @Override
    public RowRecordDecoder getRowRecordDecoder(Configuration config) {
        return new DefaultRowRecordDecoder<>(config, new TextRecordDecoder(new ClickhouseConfiguration(config)));
    }

    @Override
    public void write(String host, String hostIndex, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        if(!tempTable.contains(".")){
            tempTable = tempDatabase + "." + tempTable;
        }
        logger.info("Output result: "+hostIndex+"@"+host+"-->"+tempTable);
        context.write(new Text(hostIndex+"@"+host), new Text(tempTable));
    }
}
