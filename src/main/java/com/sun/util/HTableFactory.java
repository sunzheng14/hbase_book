package com.sun.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class HTableFactory {

    public static HTable createTable(String tableName) throws IOException {
        // 创建所需的配置
        Configuration conf = HBaseConfiguration.create();
        // 实例化一个新的客户端
        HTable table = new HTable(conf,tableName);

        return table;
    }

}
