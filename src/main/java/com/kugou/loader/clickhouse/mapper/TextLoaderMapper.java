package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by jaykelin on 2016/11/29.
 */
public class TextLoaderMapper extends AbstractClickhouseLoaderMapper<Object, Text, Text, Text> {

    @Override
    public String readLine(Object key, Text value, Context context) {
        ClickhouseConfiguration config = new ClickhouseConfiguration(context.getConfiguration());
        char separator = config.getFieldsTerminatedBy().charAt(0);
        String nullNonString = config.getNullNonString();
        String replaceChar = config.getReplaceChar();

        StringBuilder line = new StringBuilder();
        int index = 0;
        int start = 0;
        int end   = 0;
        String raw = value.toString();
        for(int i = 0; i < raw.length(); i++){
            if(raw.charAt(i) == separator){
                end = i;
                String field = raw.substring(start, end);
                if (index == clickhouseDistributedTableShardingKeyIndex){
                    clickhouseDistributedTableShardingKeyValue = field;
                }
                start = i+1;
                if (getExcludeFieldIndexs().contains(index++)){
                    continue;
                }

                if(line.length() > 0){
                    line.append(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR);
                }
                if(field.equals("\\\\N") || field.equals("/N")){
                    raw = StringUtils.isBlank(nullNonString)?ConfigurationOptions.DEFAULT_RESULT_NULL_NON_STRING:nullNonString;
                }else{
                    field = field.replace(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR, replaceChar.charAt(0));
                    field = field.replace('\\', '/');
                }
                line.append(field);
            }
        }
        String field = null;
        if (start == raw.length()){
            field = "";
        }else if(start < raw.length()){
            field = raw.substring(start);
        }

        if (index == clickhouseDistributedTableShardingKeyIndex){
            clickhouseDistributedTableShardingKeyValue = field;
        }

        line.append(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR).append(field);

        return line.toString();
    }

    @Override
    public void write(String host, String hostIndex, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        if(!tempTable.contains(".")){
            tempTable = tempDatabase + "." + tempTable;
        }
        context.write(new Text(hostIndex+"@"+host), new Text(tempTable));
    }
}
