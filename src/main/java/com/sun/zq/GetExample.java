package com.sun.zq;

import com.google.common.collect.Lists;
import com.sun.util.HTableFactory;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExample {
    public static void main(String[] args) throws IOException {
        //GetExample.getTest();
        //GetExample.getListTest();
        GetExample.getRowOrBeforeTest();
    }

    public static void getTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");

        Get get = new Get(Bytes.toBytes("row1"));
        get = get.addFamily(Bytes.toBytes("cf"));

        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col1"));
        String val = Bytes.toString(value);
        System.out.println(val);

        System.out.println(result.isEmpty());

        System.out.println(result.size());

        System.out.println(Bytes.toString(result.getRow()));

        System.out.println(result.raw().toString());
    }

    public static void getListTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        byte[] row = Bytes.toBytes("row1");
        byte[] fam = Bytes.toBytes("cf");
        byte[] col1 = Bytes.toBytes("col1");
        byte[] col2 = Bytes.toBytes("col2");

        List<Get> getList = Lists.newArrayList();
        Get get = new Get(row);
        get.addColumn(fam, col1);
        getList.add(get);

        get = new Get(row);
        get.addColumn(fam, col2);
        getList.add(get);

        Result[] results = table.get(getList);
        for (Result result : results) {
            System.out.println(Bytes.toString(result.getRow()));

            if (result.containsColumn(fam, col1)) {
                System.out.println(Bytes.toString(result.getValue(fam, col1)));
            }
            if (result.containsColumn(fam, col2)) {
                System.out.println(Bytes.toString(result.getValue(fam, col2)));
            }
        }

        for (Result result : results) {
            for (KeyValue kv : result.raw()) {
                System.out.println("Row:" + Bytes.toString(kv.getRow()) + " ,value:" + Bytes.toString(kv.getValue()));
            }
        }
    }

    public static void getRowOrBeforeTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        byte[] row = Bytes.toBytes("row2");
        byte[] fam = Bytes.toBytes("cf");
        byte[] col1 = Bytes.toBytes("col1");
        byte[] col2 = Bytes.toBytes("col2");

        Get get = new Get(row);
        get.addColumn(fam, col1);

        Result result = table.getRowOrBefore(row, fam);
        for (KeyValue kv : result.raw()) {
            System.out.println("row:"+Bytes.toString(kv.getRow()) + ", value:"+Bytes.toString(kv.getValue()));
        }
        System.out.println(Bytes.toString(result.getRow()));

    }
}
