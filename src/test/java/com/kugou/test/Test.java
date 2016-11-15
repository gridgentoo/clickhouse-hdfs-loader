package com.kugou.test;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class Test {

    public static void main(String[] args){
//        String table = "default.demo";
//        if(table.contains(".")){
//            System.out.println(table.substring(0, table.indexOf(".")));
//        }
//        String ddl = "CREATE TABLE default.demo ( dt Date,  name String,  value UInt64) ENGINE = MergeTree(dt, (dt, name), 8192)";
//        ddl = ddl.replace("default.demo","xx.xx");
//        ddl = ddl.substring(0, ddl.indexOf("=")+1);
//        ddl+= " StripeLog";
//        System.out.println(ddl);

        String taskid = "task_1477024973709_128433_m_000001";
        System.out.println(taskid.substring(taskid.indexOf("m_")));
    }
}
