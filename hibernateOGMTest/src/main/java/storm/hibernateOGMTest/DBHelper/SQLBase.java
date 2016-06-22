package storm.hibernateOGMTest.DBHelper;

import java.util.List;
import java.util.Map;

public interface SQLBase extends DBBase {
	public static final String TAG = "MYSQL";
	public List<Map<String,Object>> excuteQuery(String sql);
	public boolean insertOrUpdate(String sql);
}

