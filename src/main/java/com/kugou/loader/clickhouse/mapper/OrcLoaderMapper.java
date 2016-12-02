package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by jaykelin on 2016/11/24.
 */
public class OrcLoaderMapper extends AbstractClickhouseLoaderMapper<NullWritable, OrcStruct, Text, Text>{

    @Override
    public String readLine(NullWritable key, OrcStruct value, Context context) {
        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
        String nullNonString = "";
        String nullString = "";
        String replaceChar = clickhouseJDBCConfiguration.getReplaceChar();
        StringBuilder row = new StringBuilder();
        for(int i = 0; i < value.getNumFields(); i++){
            if(i != 0) {
                row.append(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR);
            }
            WritableComparable fieldVaule = value.getFieldValue(i);
            String field;
            if(null == fieldVaule){
                field = nullString;
            }else{
                field = fieldVaule.toString();
                if (i == clickhouseDistributedTableShardingKeyIndex){
                    clickhouseDistributedTableShardingKeyValue = field;
                }
                if(field.equals("\\N")){
                    field = nullNonString;
                }else if(field.equalsIgnoreCase("NULL")) {
                    field = nullString;
                }else {
                    field = field.replace(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR, replaceChar.charAt(0));
                    field = field.replace('\\', '/');
                }
            }
            row.append(field);
        }

        return row.toString();
    }

    @Override
    public void write(String host, String hostIndex, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        if(!tempTable.contains(".")){
            tempTable = tempDatabase + "." + tempTable;
        }
        context.write(new Text(hostIndex+"@"+host), new Text(tempTable));
    }
}
