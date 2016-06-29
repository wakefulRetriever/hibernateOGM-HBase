package storm.hibernateOGMTest.DBHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

abstract public class AbstractConfiguredSQLBase implements SQLBase {

	public static final String DRAIVE_CLASS_NAME = "com.mysql.jdbc.Driver";
	public static final String OGM_PROPERTIES_PATH = "ogm.properties";

	public static final String KEY_PACKAGE_NAME = "packageName";
	public static final String KEY_HBASE_HOST = "hbaseHost";
	public static final String KEY_SQL_URL = "sqlUrl";
	public static final String KEY_SQL_USER = "sqlUser";
	public static final String KEY_SQL_PASSWORD = "sqlPassword";

	private Connection sqlCOnnection;
	private Properties properties;
	private Statement stmt;
	private String packageName = "";

	/**
	 * sql数据库的数据库名－所有字段映射
	 */
	private Map<String, String[]> sqlFiledMap = new HashMap<>();

	public AbstractConfiguredSQLBase() {
		// TODO Auto-generated constructor stub
		initProperty();
		configureAndInit();
	}

	@Override
	public void configureAndInit() {
		// TODO Auto-generated method stub
		Map<String, String> configures = new HashMap<>();

		configures.put(KEY_SQL_URL, properties.getProperty(KEY_SQL_URL));
		configures.put(KEY_SQL_USER, properties.getProperty(KEY_SQL_USER));
		configures.put(KEY_SQL_PASSWORD, properties.getProperty(KEY_SQL_PASSWORD));
		configures.put(KEY_PACKAGE_NAME, properties.getProperty(KEY_PACKAGE_NAME));
		configureAndInit(configures);
	}

	@Override
	public void configureAndInit(Map<String, String> configures) {
		// TODO Auto-generated method stub
		packageName = configures.get(KEY_PACKAGE_NAME);
		if (packageName == null)
			throw new RuntimeException("packageName is null");
		try {
			Class.forName(DRAIVE_CLASS_NAME);// 指定连接类型
			sqlCOnnection = DriverManager.getConnection(configures.get(KEY_SQL_URL), configures.get(KEY_SQL_USER),
					configures.get(KEY_SQL_PASSWORD));// 获取连接
			stmt = sqlCOnnection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initProperty() {
		properties = new Properties();
		FileInputStream inputStream = null;
		try {
			// inputStream = new FileInputStream(OGM_PROPERTIES_PATH);
			properties.load(this.getClass().getResourceAsStream("/" + OGM_PROPERTIES_PATH));
			// inputStream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("ogm.property is not exist!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("property read exception!");
		} finally {
			if (inputStream != null)
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	}

	@Override
	public boolean checkExist(String table) {
		// TODO Auto-generated method stub
		String dbName = getProperties().getProperty("dbName");
		ResultSet rs = null;
		try {
			DatabaseMetaData metaData = this.sqlCOnnection.getMetaData();
			rs = metaData.getTables(null, dbName, null, new String[] { "TABLE" });
			for (boolean more = rs.next(); more; more = rs.next()) {
				String s = rs.getString("TABLE_NAME");
				System.out.println("tableName:" + s);
				if (s.equals(table))
					return true;
			}

			return false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return false;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public Statement getStmt() {
		return stmt;
	}

	public void setStmt(Statement stmt) {
		this.stmt = stmt;
	}

	public Map<String, String[]> getSqlFiledMap() {
		return sqlFiledMap;
	}

	public void setSqlFiledMap(Map<String, String[]> sqlFiledMap) {
		this.sqlFiledMap = sqlFiledMap;
	}

}
