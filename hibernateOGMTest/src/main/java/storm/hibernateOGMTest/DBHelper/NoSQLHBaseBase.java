package storm.hibernateOGMTest.DBHelper;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public interface NoSQLHBaseBase extends DBBase{
	public boolean tableIsExist(String tableName);
	public NoSQLHBaseTableBase createTable(String tableName,String[] columnFamily);
	public NoSQLHBaseTableBase accessTable(String tableName);
	public void closeTable(HTable table);
}
