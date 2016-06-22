package storm.hibernateOGMTest.DBHelper.impl;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import storm.hibernateOGMTest.DBHelper.NoSQLHBaseTableBase;

public class NoSQLHBaseTableBaseImpl implements NoSQLHBaseTableBase {
	private HTable mTable;

	public NoSQLHBaseTableBaseImpl(HTable table) {
		this.mTable = table;
	}

	public HTable getmTable() {
		return mTable;
	}

	public void setmTable(HTable mTable) {
		this.mTable = mTable;
	}

	@Override
	public Result query(String rowkey) {
		// TODO Auto-generated method stub
		try {
			org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(rowkey.getBytes());
			Result result = mTable.get(get);
			return result;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	@Override
	public boolean insertData(Put put) {
		// TODO Auto-generated method stub
		try {
			mTable.put(put);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

}
