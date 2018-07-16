package com.sun.zq;

import com.sun.util.HTableFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteExample {

    public static void main(String[] args) throws IOException {
        DeleteExample.deleteTest();
    }

    public static void deleteTest() throws IOException {
        HTable table = HTableFactory.createTable("testtable");

        Delete del1 = new Delete(Bytes.toBytes("row1"));
        del1.deleteColumn(Bytes.toBytes("fam1"),Bytes.toBytes("col1"),3);

        Delete del2 = new Delete(Bytes.toBytes("row1"));
        del2.deleteFamily(Bytes.toBytes("fam1"),2);



        table.delete(del2);
        table.close();

    }
}
