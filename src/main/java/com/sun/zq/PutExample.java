package com.sun.zq;

import com.google.common.collect.Lists;
import com.sun.util.HTableFactory;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class PutExample {
	public static void main(String[] args) throws IOException {
		// 实例化一个新的客户端
		HTable table = HTableFactory.createTable("testtable");
		System.out.println(table.isAutoFlush());

		System.out.println(table.getWriteBufferSize());//缓冲区大小

		table.setAutoFlushTo(false);//启用客户端写缓存
		// 指定一行来创建一个put
		Put put = new Put(Bytes.toBytes("row3"));
		put.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual3"), Bytes.toBytes("val3"));
		put.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual4"), Bytes.toBytes("val4"));
		put.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual5"), Bytes.toBytes("val5"));
		// 存储到Hbase中
		try {
			table.put(put);
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		}
		table.flushCommits();//把客户端缓存中的数据强制写到服务器
		List<Put> putList = Lists.newArrayList();
		put = new Put(Bytes.toBytes("row2"));
		put.add(Bytes.toBytes("fam1"),Bytes.toBytes("col1"),Bytes.toBytes("value1"));
		put.add(Bytes.toBytes("fam1"),Bytes.toBytes("col2"),Bytes.toBytes("value2"));
		putList.add(put);

		//table.put(putList);
		//table.flushCommits();

		Get get = new Get(Bytes.toBytes("row2"));
		Result result = table.get(get);
		System.out.println(result);
	}

	/*private static void createTable() {
		String tablename = "sz";
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename.getBytes())) {
			System.out.println("table exists");
		} else {
			HTableDescriptor hd = new HTableDescriptor(tablename);
			hd.addFamily(new HColumnDescriptor(columnFamily.getBytes()));
			admin.createTable(hd);

			System.out.println("create table success");
		}
	}*/

}
