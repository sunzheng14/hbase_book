package com.sun.zq;

import com.sun.util.HTableFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CheckAndPutExample {
    public static void main(String[] args) throws IOException {
        HTable table = HTableFactory.createTable("sz");
        table.setAutoFlushTo(false);

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.add(Bytes.toBytes("cf"),Bytes.toBytes("col2"),Bytes.toBytes("val2"));

        boolean res1 = table.checkAndPut(Bytes.toBytes("row1"),Bytes.toBytes("cf"),Bytes.toBytes("col2"),Bytes.toBytes("val2"), put1);
        System.out.println(res1);

        boolean res2 = table.checkAndPut(Bytes.toBytes("row1"),Bytes.toBytes("cf"),Bytes.toBytes("col2"),Bytes.toBytes("val5"), put1);
        System.out.println(res2);

    }

}
