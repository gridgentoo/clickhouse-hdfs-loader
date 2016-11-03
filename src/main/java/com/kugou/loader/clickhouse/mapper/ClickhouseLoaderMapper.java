package com.kugou.loader.clickhouse.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * Created by jaykelin on 2016/11/1.
 */
public class ClickhouseLoaderMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {


    @Override
    protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
        ClickhouseJDBCConfiguration clickhouseJDBCConfiguration = new ClickhouseJDBCConfiguration(context.getConfiguration());
        String nullNonString = clickhouseJDBCConfiguration.getNullNonString();
        String nullString = clickhouseJDBCConfiguration.getNullString();
        String fieldsTerminatedBy = clickhouseJDBCConfiguration.getFieldsTerminatedBy();
        String replaceChar = clickhouseJDBCConfiguration.getReplaceChar();
        String dt = clickhouseJDBCConfiguration.getDt();
        StringBuffer valsb = new StringBuffer();
        for(int i = 0; i < value.getNumFields(); i++){
            WritableComparable fieldVaule = value.getFieldValue(i);
            String field = null;
            if(null == fieldVaule){
                field = nullString;
            }else{
                field = fieldVaule.toString();
                if(i != 0) {
                    valsb.append('\t');
                }
                if(field.equals("\\N")){
                    field = nullNonString;
                }else{
                    field = field.replace('\t', replaceChar.charAt(0));
                }
            }

            valsb.append(field);
        }
        valsb.append('\t').append(dt).append('\n');
//        String line = value.toString();
//        line = line.replace('\t', replaceChar.charAt(0))
//                .replace(fieldsTerminatedBy.toCharArray()[0], '\t')
//                .replace("\\N", nullNonString);

        context.write(NullWritable.get(), new Text(valsb.toString()));
    }
}
