package com.sun.zq;

import com.sun.util.HTableFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanExample {
    public static void main(String[] args) throws IOException {
        HTable table = HTableFactory.createTable("testtable");
        System.out.println(table.getScannerCaching());
        Scan scan = new Scan();
        System.out.println(scan.getCaching());
        System.out.println(Bytes.toString(table.getTableName()));

        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        TableName tableName = tableDescriptor.getTableName();
        System.out.println(Bytes.toString(tableName.getName()));



        ResultScanner rs = table.getScanner(scan);

        for (Result res : rs){
            System.out.println(res);
        }
        rs.close();

        scan.addFamily(Bytes.toBytes("fam1"));
        rs = table.getScanner(scan);

        for (Result res : rs){

            System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("fam1"),Bytes.toBytes("qual1"))));
        }







    }
}
