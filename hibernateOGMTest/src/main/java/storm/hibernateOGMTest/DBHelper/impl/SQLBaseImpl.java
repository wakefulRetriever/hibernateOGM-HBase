package storm.hibernateOGMTest.DBHelper.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import storm.hibernateOGMTest.DBHelper.AbstractConfiguredSQLBase;

/**
 * @author storm SQL数据库的实现类
 */
public class SQLBaseImpl extends AbstractConfiguredSQLBase {

	@Override
	public Map<String, Object> excuteQuery(String table, String sql) {
		// TODO Auto-generated method stub
		Map<String, Object> result = new HashMap<>();
		ResultSet rs = null;
		try {
			System.out.println("execute sql:" + sql);
			rs = getStmt().executeQuery(sql);
			System.out.println(rs.toString());
			if (rs == null)
				return null;

			String[] columnNames = getSqlFiledMap().get(table);
			if (columnNames == null) {
				String ClASS_NAME = getProperties().getProperty(KEY_PACKAGE_NAME) + "." + table;
				Class clazz = Class.forName(ClASS_NAME);
				Field[] fields = clazz.getDeclaredFields();
				int j = 0;
				String[] columns = new String[fields.length - 1];
				for (Field f : fields) {
					if (!f.getName().equals("id"))
						columns[j++] = f.getName();
				}
				getSqlFiledMap().put(table, columns);
				columnNames = columns;
			}

			for (boolean more = rs.next(); more; more = rs.next()) {
				for (String column : columnNames) {

					String s = rs.getString(column);
					result.put(column, s);

				}
			}

			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return result;
		} catch (ClassNotFoundException e) {
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
		return result;
	}

	@Override
	public boolean insert(String table, String sql) {
		// TODO Auto-generated method stub
		try {
			System.out.println("insert execute SQL:" + sql);
			int res = getStmt().executeUpdate(sql);
			// getStmt().close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("insert " + table + " failed! sql:" + sql);
		}

	}

	@Override
	public boolean update(String table, String sql) {
		// TODO Auto-generated method stub
		try {
			System.out.println("update execute sql:" + sql);
			getStmt().executeUpdate(sql);
			// getStmt().close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("update " + table + " failed! sql:" + sql);
		}

	}

	// CREATE TABLE Latter(
	// id varchar(50) PRIMARY KEY,
	// LastName varchar(255),
	// FirstName varchar(255),
	// Address varchar(255),
	// City varchar(255)
	// )ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
	@Override
	public boolean create(String table, String[] column) {
		String sql = String.format("CREATE TABLE %s(id varchar(50) PRIMARY KEY,", table);
		for (String s : column) {
			sql += s + " varchar(255),";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;";
		System.out.println("create sql:" + sql);

		getSqlFiledMap().put(table, column);
		System.out.println(getSqlFiledMap().toString());
		System.out.println();
		try {
			return getStmt().execute(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("sql create table failed!");
		}

	}

}
