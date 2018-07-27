package com.sun.zq;

import com.sun.util.HTableFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterExample {
    public static void main(String[] args) throws IOException {
        //FilterExample.rowFilterTest();
        //FilterExample.familyFilterTest();
        //FilterExample.qualifierFilterTest();
        //FilterExample.valueFilterTest();
        //FilterExample.singleColumnValueFilterTest();
        inclusiveStopFilterTest();
    }

    public static void rowFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        Filter filter = new RowFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes("row2")));
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result rs : resultScanner) {
            System.out.println(rs);
        }

        filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("2"));
        scan.setFilter(filter);
        resultScanner = table.getScanner(scan);
        for (Result rs : resultScanner) {
            System.out.println(rs);
        }
        resultScanner.close();
    }

    public static void familyFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");

        Filter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("cf")));
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanResult = table.getScanner(scan);
        for (Result rs : scanResult) {
            System.out.println(rs);
        }

        Get get = new Get(Bytes.toBytes("row3"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

        scanResult.close();
    }

    public static void qualifierFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("col1")));

        Scan scan = new Scan();
        scan.setFilter(filter);

        table.getScanner(scan);
        ResultScanner scanResult = table.getScanner(scan);
        for (Result rs : scanResult) {
            System.out.println(rs);
        }

        Get get = new Get(Bytes.toBytes("row1"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

        scanResult.close();
    }

    public static void valueFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("2"));
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (KeyValue kv : result.raw()) {
                System.out.println("kv=" + kv + ", value=" + Bytes.toString(kv.getValue()));
            }
        }

        Get get = new Get(Bytes.toBytes("row1"));
        get.setFilter(filter);
        Result result = table.get(get);
        for (KeyValue kv : result.raw()) {
            System.out.println("kv=" + kv + ", value=" + Bytes.toString(kv.getValue()));
        }
        resultScanner.close();
    }

    public static void singleColumnValueFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("col1"), CompareOp.EQUAL,
                        new SubstringComparator("1"));
        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (KeyValue kv : result.raw()) {
                System.out.println("kv=" + kv + ", value=" + Bytes.toString(kv.getValue()));
            }
        }
        resultScanner.close();
    }

    public static void inclusiveStopFilterTest() throws IOException {
        HTable table = HTableFactory.createTable("sz");
        Filter filter = new InclusiveStopFilter(Bytes.toBytes("row4"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setStartRow(Bytes.toBytes("row2"));
        //scan.setStopRow(Bytes.toBytes("row4"));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (KeyValue kv : result.raw()) {
                System.out.println("kv=" + kv + ", value=" + Bytes.toString(kv.getValue()));
            }
        }
        resultScanner.close();




    }
}
