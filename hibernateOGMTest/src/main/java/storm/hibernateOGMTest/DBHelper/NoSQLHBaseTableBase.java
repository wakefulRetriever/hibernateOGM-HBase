package storm.hibernateOGMTest.DBHelper;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface NoSQLHBaseTableBase {
	
	public Result query(String rowkey);
	public boolean insertData(Put put);
	
}
