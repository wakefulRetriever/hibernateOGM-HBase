package storm.hibernateOGMTest.DBHelper;

import java.util.Map;

public interface DBBase{
	public void configureAndInit(Map<String,String> configures);
}
