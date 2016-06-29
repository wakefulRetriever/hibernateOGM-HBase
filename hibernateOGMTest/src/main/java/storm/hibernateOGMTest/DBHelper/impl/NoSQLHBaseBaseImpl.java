package storm.hibernateOGMTest.DBHelper.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import storm.hibernateOGMTest.DBHelper.AbstractConfiguredNoSQLHbaseBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseTableBase;

public class NoSQLHBaseBaseImpl extends AbstractConfiguredNoSQLHbaseBase {
	

	public NoSQLHBaseBaseImpl() {
		// TODO Auto-generated constructor stub
		// configureAndInit(configures);
		Map<String, String> configures = new HashMap<>();
		configureAndInit();
	}

	@Override
	public boolean tableIsExist(String tableName) {
		// TODO Auto-generated method stub
		org.apache.hadoop.hbase.client.HBaseAdmin admin = null;
		try {
			admin = new org.apache.hadoop.hbase.client.HBaseAdmin(getHbaseConfiguration());
			if (tableName != null && !admin.tableExists(tableName.getBytes())) {
				return false;
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (admin != null)
				try {
					admin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	@Override
	public NoSQLHBaseTableBase createTable(String tableName, String[] columnFamily) {
		// TODO Auto-generated method stub
		org.apache.hadoop.hbase.client.HBaseAdmin admin = null;
		try {
			admin = new org.apache.hadoop.hbase.client.HBaseAdmin(getHbaseConfiguration());
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (String family : columnFamily)
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDescriptor);
			admin.close();
			HTable table = new HTable(getHbaseConfiguration(), tableName);
			NoSQLHBaseTableBase hbaseTable = new NoSQLHBaseTableBaseImpl(table);
			
			return hbaseTable;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void closeTable(HTable table) {
		// TODO Auto-generated method stub
		if(null != table)
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	@Override
	public NoSQLHBaseTableBase accessTable(String tableName) {
		// TODO Auto-generated method stub
		HTable table;
		try {
			table = new HTable(getHbaseConfiguration(), tableName);
			NoSQLHBaseTableBase hbaseTable = new NoSQLHBaseTableBaseImpl(table);
			return hbaseTable;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}



}
