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
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseTableBase;

public class NoSQLHBaseBaseImpl implements NoSQLHBaseBase {
	private static final String TEST_HOST = "10.108.209.95";
	private Configuration mConf = null;

	public NoSQLHBaseBaseImpl() {
		// TODO Auto-generated constructor stub
		// configureAndInit(configures);
		Map<String, String> configures = new HashMap<>();
		configures.put("host", TEST_HOST);
		configureAndInit(configures);
	}

	@Override
	public void configureAndInit(Map<String, String> configures) {
		// TODO Auto-generated method stub
		String ip = configures.get("host");
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ip);
		// 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		mConf = HBaseConfiguration.create(conf);
	}

	@Override
	public boolean tableIsExist(String tableName) {
		// TODO Auto-generated method stub
		org.apache.hadoop.hbase.client.HBaseAdmin admin = null;
		try {
			admin = new org.apache.hadoop.hbase.client.HBaseAdmin(mConf);
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
			admin = new org.apache.hadoop.hbase.client.HBaseAdmin(mConf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (String family : columnFamily)
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDescriptor);
			admin.close();
			HTable table = new HTable(mConf, tableName);
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
			table = new HTable(mConf, tableName);
			NoSQLHBaseTableBase hbaseTable = new NoSQLHBaseTableBaseImpl(table);
			return hbaseTable;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}



}
