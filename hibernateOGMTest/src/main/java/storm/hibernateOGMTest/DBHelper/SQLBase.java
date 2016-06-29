package storm.hibernateOGMTest.DBHelper;

import java.util.List;
import java.util.Map;

/**
 * @author storm
 * SQL数据库的接口类
 */
public interface SQLBase extends DBBase {
	public static final String TAG = "MYSQL";
	public Map<String,Object> excuteQuery(String table,String sql);
	public boolean create(String table,String[] column);
	public boolean insert(String table,String sql);
	public boolean update(String table,String sql);
	public boolean checkExist(String table);
}

