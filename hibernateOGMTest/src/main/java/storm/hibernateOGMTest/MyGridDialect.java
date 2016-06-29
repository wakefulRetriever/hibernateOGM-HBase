package storm.hibernateOGMTest;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.hamcrest.core.IsInstanceOf;
import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.ogm.datastore.infinispan.InfinispanDialect;
import org.hibernate.ogm.datastore.infinispan.impl.InfinispanDatastoreProvider;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.dialect.spi.AssociationTypeContext;
import org.hibernate.ogm.dialect.spi.BaseGridDialect;
import org.hibernate.ogm.dialect.spi.DuplicateInsertPreventionStrategy;
import org.hibernate.ogm.dialect.spi.ModelConsumer;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

import storm.hibernateOGMTest.DBHelper.AbstractConfiguredSQLBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseTableBase;
import storm.hibernateOGMTest.DBHelper.SQLBase;
import storm.hibernateOGMTest.DBHelper.impl.NoSQLHBaseBaseImpl;
import storm.hibernateOGMTest.DBHelper.impl.SQLBaseImpl;
import storm.hibernateOGMTest.myannotation.HBase;
import storm.hibernateOGMTest.myannotation.MySql;
import storm.hibernateOGMTest.util.JoinedDataHandler;

public class MyGridDialect extends BaseGridDialect {
	public static final String DOMAIN_PACKAGE_NAME = "packageName";
	private static NoSQLHBaseBase hbaseAdmin = new NoSQLHBaseBaseImpl();
	private static final String DEFAULT_TEST_PACKGE = "storm.hibernateOGMTest.domain";
	private JoinedDataHandler handler;
	private SQLBase sqlBase = new SQLBaseImpl();
	private NoSQLHBaseBase noSqlBase = new NoSQLHBaseBaseImpl();
	private Map<String, Boolean> sqlTableExist = new HashMap<>();

	public MyGridDialect(MyDatastoreProvider provider) {
		// super(provider);
		System.out.println("MyGridDialect");
		// TODO Auto-generated constructor stub
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		// TODO Auto-generated method stub
		Class clazz;
		boolean isHBaseTable = false, isMySqlTable = false;
		String className = ((AbstractConfiguredSQLBase)sqlBase).getProperties().getProperty(DOMAIN_PACKAGE_NAME) + "." + key.getTable();
		try {
			clazz = Class.forName(className);
			Annotation[] as = clazz.getAnnotations();
			for (Annotation a : as) {
				if (a instanceof HBase) {
					isHBaseTable = true;
					isMySqlTable = false;
					break;
				}
				if (a instanceof MySql) {
					isHBaseTable = false;
					isMySqlTable = true;
					break;
				}
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("ClassNotFoundException className:" + className);
		}

		if (isHBaseTable) {
			return HandleNoSQLProcessGet(key, tupleContext);
		} else if (isMySqlTable) {
			return HandleSQLProcessGet(key, tupleContext);
		}

		return null;
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		// TODO Auto-generated method stub
		System.out.println("========enter createTuple========");
		for (String s : key.getColumnNames())
			System.out.println(s);
		for (Object s : key.getColumnValues())
			System.out.println(s);
		System.out.println("========exit createTuple========");

		return new Tuple();
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) {
		// TODO Auto-generated method stub
		System.out.println("----------------insertOrUpdateTuple----------------");
		Class clazz;
		boolean isHBaseTable = false, isMySqlTable = false;
		for (String s : tuple.getColumnNames())
			System.out.println(s + "  " + tuple.get(s));
		String className = ((AbstractConfiguredSQLBase)sqlBase).getProperties().getProperty(DOMAIN_PACKAGE_NAME) + "." + key.getTable();
		try {
			//
			
			clazz = Class.forName(className);
			Annotation[] as = clazz.getAnnotations();
			for (Annotation a : as) {
				if (a instanceof HBase) {
					isHBaseTable = true;
					isMySqlTable = false;
				}
				if (a instanceof MySql) {
					isHBaseTable = false;
					isMySqlTable = true;
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("class not found class name:" + className);
		}

		if (isHBaseTable) {
			handleNoSQLProcessInsert(key, tuple,tupleContext);
		} else if (isMySqlTable) {
			handleSQLProcessInsert(key,tuple, tupleContext);
		}

		// if (this.handler == null || !this.handler.isValid())
		// try {
		//
		// Class<?> clazz = Class.forName("storm.hibernateOGMTest.domain." +
		// key.getTable());
		// Set<String> familyNames = new HashSet<>();
		// for (Field f : clazz.getDeclaredFields()) {
		// System.out.println(f.getName());
		// if (!f.getName().equals("id"))
		// familyNames.add(f.getName());
		// // f.getC
		// }
		// ;
		// handler = new JoinedDataHandler(familyNames);
		// handler.setRowkey(key.getColumnValues()[0].toString());
		// handler.setValid(true);
		// handler.setTableName(key.getTable());
		// } catch (ClassNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("========enter insertOrUpdateTuple========");
		//
		// System.out.println("insertOrUpdateTuple:");
		// Set<String> set = tuple.getColumnNames();
		//
		// for (String s : set) {
		// String[] cor = s.split("[\\.]");
		// if (cor.length != 2)
		// continue;
		// String value = tuple.get(s).toString();
		// handler.addData(cor[0], cor[1], value);
		// }
		//
		// if (handler.isCompleted()) {
		// System.out.println("*****will send...*****");
		// System.out.println(handler.toString());
		//
		// System.out.println("*****has sended!*****");
		// }
		// System.out.println(set);
		//
		// System.out.println("========exit insertOrUpdateTuple========");
		// super.insertOrUpdateTuple(key, tuple, tupleContext);
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		// TODO Auto-generated method stub
		System.out.println("removeTuple");
		// super.removeTuple(key, tupleContext);

	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		// TODO Auto-generated method stub
		System.out.println("------------enter getAssociation--------");
		Association association = new Association();

		System.out.println(key.toString());
		System.out.println(associationContext.toString());
		// RowKey rk = new RowKey(columnNames, columnValues)
		// association.put(key, value);

		System.out.println("------------exit getAssociation-------------");

		return new Association();
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
		// TODO Auto-generated method stub
		System.out.println("createAssociation");
		return null;
	}

	@Override
	public void insertOrUpdateAssociation(AssociationKey key, Association association,
			AssociationContext associationContext) {
		// TODO Auto-generated method stub
		System.out.println("-----------------enter insertOrUpdateAssociation-------------");

		System.out.println(association.toString());

		// System.out.println(handler.getFamilyNames());
		// System.out.println(handler.getFamilys().keySet());
		// if (handler.isCompleted()) {
		// System.out.println("*****will send...*****");
		// System.out.println(handler.toString());
		// NoSQLHBaseTableBase tableAdmin = null;
		// if (!this.hbaseAdmin.tableIsExist(handler.getTableName())) {
		// System.out.println(handler.getTableName()+ " exist...");
		// String[] columnFamily = new String[handler.getFamilyNames().size()];
		// handler.getFamilyNames().toArray(columnFamily);
		// System.out.println(columnFamily.length);
		// tableAdmin = this.hbaseAdmin.createTable(handler.getTableName(),
		// columnFamily);
		// }
		//
		// tableAdmin = this.hbaseAdmin.accessTable(handler.getTableName());
		// if (tableAdmin == null) {
		// System.out.println("send error...");
		// } else {
		// tableAdmin.insertData(handler.transfromToHBasePut());
		//
		// System.out.println("*****has sended!*****");
		// }
		// }
		// System.out.println("========exit insertOrUpdateAssociation========");
		// association.get()

		// super.insertOrUpdateAssociation(key, association,
		// associationContext);
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
		// TODO Auto-generated method stub
		System.out.println("removeAssociation");
		// super.removeAssociation(key, associationContext);
	}

	@Override
	public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata,
			AssociationTypeContext associationTypeContext) {
		// TODO Auto-generated method stub
		// System.out.println("isStoredInEntityStructure");
		return false;
	}

	@Override
	public Number nextValue(NextValueRequest request) {
		// TODO Auto-generated method stub
		System.out.println("nextValue");
		return 0;
	}

	@Override
	public void forEachTuple(ModelConsumer consumer, TupleContext tupleContext, EntityKeyMetadata entityKeyMetadata) {
		// TODO Auto-generated method stub
		// System.out.println("forEachTuple");
		// super.forEachTuple(consumer, tupleContext, entityKeyMetadata);
	}

	@Override
	public DuplicateInsertPreventionStrategy getDuplicateInsertPreventionStrategy(EntityKeyMetadata entityKeyMetadata) {
		// TODO Auto-generated method stub
		// System.out.println("DuplicateInsertPreventionStrategy");
		return null;
	}

	@Override
	public boolean supportsSequences() {
		// TODO Auto-generated method stub
		// System.out.println("supportsSequences");
		return true;
	}

	public static void main(String[] args) {
		String s = "s.a";
		String[] res = s.split("[\\.]");
		System.out.println(res.length);
	}

	private Tuple HandleNoSQLProcessGet(EntityKey key, TupleContext context) {
		NoSQLHBaseTableBase tableBase = noSqlBase.accessTable(key.getTable());
		String rowkey = (String) key.getColumnValues()[0];
		if (rowkey == null)
			throw new RuntimeException("rowkey is null !");
		Tuple tuple = new Tuple();
		Result result = tableBase.query(rowkey);

		for (String name : context.getSelectableColumns()) {

			String[] splited = name.split("[\\.]");
			if (splited.length != 2)
				throw new RuntimeException("columnFamily or key is absent !");
			String value = new String(
					result.getColumnLatestCell(splited[0].getBytes(), splited[1].getBytes()).getValue());
			tuple.put(name, value);
			System.out.println("key:" + name + " value:" + value);
		}
		System.out.println(tuple.toString());
		return tuple;
	}

	private void handleNoSQLProcessInsert(EntityKey key, Tuple tuple,TupleContext context) {

	}

	private Tuple HandleSQLProcessGet(EntityKey key, TupleContext context) {
		String tableName = key.getTable();
		if(!this.sqlTableExist.getOrDefault(tableName, false) || !sqlBase.checkExist(tableName)){
			createIfNotExist(tableName);
		}
		String id = (String) key.getColumnValues()[0];
		String sql = String.format("SELECT * FROM %s where id ='%s';",tableName,id);
		Map<String ,Object> map = sqlBase.excuteQuery(tableName, sql);
		Tuple tuple = new Tuple();
		for(String s : context.getSelectableColumns()){
			tuple.put(s, map.get(s));
		}
		
		
		return tuple;
	}

	private void handleSQLProcessInsert(EntityKey key, Tuple tuple,TupleContext tupleContext) {
		String tableName = key.getTable();
		createIfNotExist(tableName);
		//INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
		String sql = String.format("INSERT INTO %s(", tableName);
		for(String column : tuple.getColumnNames()){
			sql += column +",";
		}
		sql = sql .substring(0, sql.length() - 1);
		sql += ") VALUES(";
		
		for(String column : tuple.getColumnNames()){
			Object value = tuple.get(column);
			sql += "'"+value.toString()+"',";
		}
		sql = sql.substring(0,sql.length() - 1);
		sql += ");";
	
		
		sqlBase.insert(tableName, sql);
	}

	private void createIfNotExist(String tableName) {
		if (!sqlTableExist.getOrDefault(tableName, false) && !sqlBase.checkExist(tableName)) {
			System.out.println("table " + tableName + "is not existed !");
			String packageName = ((AbstractConfiguredSQLBase) sqlBase).getProperties().getProperty("packageName");
			if (packageName == null)
				packageName = DEFAULT_TEST_PACKGE;
			String className = packageName + "." + tableName;
			try {
				Class clazz = Class.forName(className);
				Field[] fields = clazz.getDeclaredFields();
				String[] columns = new String[fields.length - 1];
				for (int i = 0,j = 0; i < fields.length; i++){
					if( fields[i].getName().equals("id"))
						continue;
					columns[j ++] =fields[i].getName();
					
				}
				sqlBase.create(tableName, columns);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("can not reflect class " + className);
			}

			this.sqlTableExist.put(tableName, true);
		}
	}

}
