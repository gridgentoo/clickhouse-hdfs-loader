package com.kugou.test;

import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import org.junit.*;

import java.sql.SQLException;

/**
 * Created by jaykelin on 2017/4/18.
 */
public class CHJDBCTest {

    @org.junit.Test
    public void insertTest() throws SQLException {
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient("10.12.0.22", 38123, "default", "default", "default@kugou");
        StringBuilder query = new StringBuilder();
        query.append("Insert into test.t_lzj_test01 FORMAT TabSeparated").append("\n");
        query.append("2017-01-01").append("\t").append("JK").append("\t").append("\n");

        System.out.println(query.toString());
        client.executeUpdate(query.toString());
    }
}
