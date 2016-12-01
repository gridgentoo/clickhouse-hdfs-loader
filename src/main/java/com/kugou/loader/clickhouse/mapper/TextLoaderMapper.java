package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by jaykelin on 2016/11/29.
 */
public class TextLoaderMapper extends AbstractClickhouseLoaderMapper<Object, Text, Text, Text> {

    @Override
    public String readLine(Object key, Text value, Context context) {
        ClickhouseConfiguration config = new ClickhouseConfiguration(context.getConfiguration());
        String sperator = config.getFieldsTerminatedBy();
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), sperator);
        String dt = config.getDt();

        StringBuilder line = new StringBuilder();
        int index = 0;
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();

        }
        return null;
    }

    @Override
    public void write(String host, String hostIndex, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        if(!tempTable.contains(".")){
            tempTable = tempDatabase + "." + tempTable;
        }
        context.write(new Text(hostIndex+"@"+host), new Text(tempTable));
    }
}
