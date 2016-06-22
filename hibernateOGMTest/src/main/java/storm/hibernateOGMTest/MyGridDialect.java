package storm.hibernateOGMTest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

import storm.hibernateOGMTest.DBHelper.NoSQLHBaseBase;
import storm.hibernateOGMTest.DBHelper.NoSQLHBaseTableBase;
import storm.hibernateOGMTest.DBHelper.impl.NoSQLHBaseBaseImpl;
import storm.hibernateOGMTest.util.JoinedDataHandler;

public class MyGridDialect extends BaseGridDialect {
	private static NoSQLHBaseBase hbaseAdmin = new NoSQLHBaseBaseImpl();
	private static final String PACKGE = "storm.hibernateOGMTest.domain";
	private JoinedDataHandler handler;

	public MyGridDialect(MyDatastoreProvider provider) {
		// super(provider);
		System.out.println("MyGridDialect");
		// TODO Auto-generated constructor stub
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		// TODO Auto-generated method stub

		System.out.println(key.getMetadata().getClass());
		System.out.println("getTuple:" + key.toString());
		for (String s : key.getColumnNames())
			System.out.println(s);
		System.out.println(tupleContext.getAllRoles().toString());
		System.out.println(tupleContext.getSelectableColumns());
		System.out.println(tupleContext.getAllAssociatedEntityKeyMetadata().isEmpty());
		if (key.getColumnValues()[0].equals("4334y-45rtg-435")) {
			Tuple t = new Tuple();
			t.put("age", 11);
			t.put("master", "ldk");
			t.put("name", "ww");
			// List<String> list3 = new ArrayList<>();
			// list3.add("info1");
			// list3.add("info2");
			// t.put("list3", list3);
			return t;

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

		if (this.handler == null || !this.handler.isValid())
			try {

				Class<?> clazz = Class.forName("storm.hibernateOGMTest.domain." + key.getTable());
				Set<String> familyNames = new HashSet<>();
				for (Field f : clazz.getDeclaredFields()) {
					System.out.println(f.getName());
					if (!f.getName().equals("id"))
						familyNames.add(f.getName());
				}
				;
				handler = new JoinedDataHandler(familyNames);
				handler.setRowkey(key.getColumnValues()[0].toString());
				handler.setValid(true);
				handler.setTableName(key.getTable());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.println("========enter insertOrUpdateTuple========");

		System.out.println("insertOrUpdateTuple:");
		Set<String> set = tuple.getColumnNames();

		for (String s : set) {
			String[] cor = s.split("[\\.]");
			if (cor.length != 2)
				continue;
			String value = tuple.get(s).toString();
			handler.addData(cor[0], cor[1], value);
		}

		if (handler.isCompleted()) {
			System.out.println("*****will send...*****");
			System.out.println(handler.toString());

			System.out.println("*****has sended!*****");
		}
		System.out.println(set);

		System.out.println("========exit insertOrUpdateTuple========");
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
		System.out.println("========enter getAssociation========");
		String id = null;
		System.out.println(associationContext.getAssociationTypeContext().getRoleOnMainSide());

		// Association association = new Association(new
		// MyAssociationSnapshot());

		System.out.println("========exit getAssociation========");

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
		System.out.println("========enter insertOrUpdateAssociation========");
		Iterable<RowKey> rowkeys = association.getKeys();

		Iterator<RowKey> rowkeyIterator = rowkeys.iterator();
		while (rowkeyIterator.hasNext()) {
			RowKey rk = rowkeyIterator.next();
			System.out.println("------------tuple--------------");
			Tuple t = association.get(rk);
			String familyName = null, keyName = null, value = null;
			for (String s : t.getColumnNames()) {
				String[] ss = s.split("[\\.]");
				familyName = ss[0];
				if (ss[1].equals("key"))
					keyName = t.get(s).toString();
				else
					value = t.get(s).toString();

				// System.out.println(s + " " + );
			}
			System.out.println("family:" + familyName + " key:" + keyName + " value:" + value);
			handler.addData(familyName, keyName, value);

		}
		System.out.println(handler.getFamilyNames());
		System.out.println(handler.getFamilys().keySet());
		if (handler.isCompleted()) {
			System.out.println("*****will send...*****");
			System.out.println(handler.toString());
			NoSQLHBaseTableBase tableAdmin = null;
			if (!this.hbaseAdmin.tableIsExist(handler.getTableName())) {
				System.out.println(handler.getTableName()+ " exist...");
				String[] columnFamily = new String[handler.getFamilyNames().size()];
				 handler.getFamilyNames().toArray(columnFamily);
				System.out.println(columnFamily.length);
				tableAdmin = this.hbaseAdmin.createTable(handler.getTableName(), columnFamily);
			}
			
			tableAdmin = this.hbaseAdmin.accessTable(handler.getTableName());
			if (tableAdmin == null) {
				System.out.println("send error...");
			} else {
				tableAdmin.insertData(handler.transfromToHBasePut());

				System.out.println("*****has sended!*****");
			}
		}
		System.out.println("========exit insertOrUpdateAssociation========");
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
}
