package com.kugou.test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class Test {

    @org.junit.Test
    public void simpleTest(){
        String table = "default.demo";
        if(table.contains(".")){
            System.out.println(table.substring(table.indexOf(".") + 1));
        }
//        String ddl = "CREATE TABLE default.demo ( dt Date,  name String,  value UInt64) ENGINE = MergeTree(dt, (dt, name), 8192)";
//        ddl = ddl.replace("default.demo","xx.xx");
//        ddl = ddl.substring(0, ddl.indexOf("=")+1);
//        ddl+= " StripeLog";
//        System.out.println(ddl);

//        String taskid = "task_1477024973709_128433_m_000001";
//        System.out.println(taskid.substring(taskid.indexOf("m_")));


        String ddl = "ENGINE = Distributed(perftest_2shards_1replicas, 'default', 'dwf_list_play_d')";
        Pattern pattern = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *, *'?([A-Za-z0-9_\\-]+)'? *(, *[a-zA-Z0-9_\\-]+\\(([A-Za-z0-9_\\-]+|)\\))? *\\)$");
        Matcher m = pattern.matcher(ddl);
        if(m.find()){
            System.out.println("Found."+m.groupCount());
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));
            System.out.println(m.group(4));
            System.out.println(m.group(5));
        }

        HashFunction fn = Hashing.murmur3_128();


//        for(int j = 0; j< 100; j++){
//            long l = (long)((Math.random()*9+1)*100);
////            System.out.println(l+":"+fn.hashLong(l).asInt());
//            System.out.println(Math.abs(fn.hashString(UUID.randomUUID().toString()).asInt()) % 3);
//        }

        System.out.println(Math.abs(fn.hashString("1_10.1.0.210").hashCode()) % 3);
        System.out.println(Math.abs(fn.hashString("2_10.1.113.176").hashCode()) % 3);
        System.out.println(Math.abs(fn.hashString("3_10.1.112.63").hashCode()) % 3);

        System.out.println(("1".hashCode() & Integer.MAX_VALUE) % 3);
        System.out.println(("2".hashCode() & Integer.MAX_VALUE) % 3);
        System.out.println(("3".hashCode() & Integer.MAX_VALUE) % 3);
    }
}
