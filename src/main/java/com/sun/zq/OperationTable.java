package com.sun.zq;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class OperationTable {
	static Configuration cfg = HBaseConfiguration.create();
	
	public static void main(String[] args) throws IOException {
		String tablename ="sz";
		String columnFamily ="cf";
		
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if(admin.tableExists(tablename.getBytes())){
			System.out.println("table exists");
		}else {
			TableName tableName = TableName.valueOf(tablename);
			HTableDescriptor hd = new HTableDescriptor(tableName);
			hd.addFamily(new HColumnDescriptor(columnFamily.getBytes()));
			admin.createTable(hd);
			
			System.out.println("create table success");
		}
	}

	public static void tableTest() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(cfg);
//		admin.getMasterCoprocessors();
//		admin.isMasterRunning();
//		admin.getConnection();
//		admin.getConfiguration();
//		admin.close();



		TableName tableName = TableName.valueOf("sz");
		HTableDescriptor hd = new HTableDescriptor(tableName);
		HColumnDescriptor cf = new HColumnDescriptor(Bytes.toBytes("cf2"));
		cf.getName();
		cf.getNameAsString();
		cf.getMaxVersions();
		hd.addFamily(cf);


		admin.createTable(hd);
	}
	
	
			
}
