package storm.hibernateOGMTest;

import java.util.Iterator;

import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.AssociationSnapshot;
import org.hibernate.ogm.model.spi.Tuple;

public class MyAssociationSnapshot implements AssociationSnapshot {
	String[] columnNames = { "Dog_id", "list2.key", "list2.value" };
	String[] columnValues1 = new String[] { "4334y-45rtg-435", "keykey1", "valuevalue1" };


	@Override
	public Tuple get(RowKey arg0) {
		System.out.println("get:"+arg0.toString());
		Tuple t = new Tuple();
		for (int i = 0; i < columnNames.length; i++)
			t.put(columnNames[i], columnValues1[i]);
		return t;
	}

	@Override
	public boolean containsKey(RowKey arg0) {
		System.out.println("containsKey:"+arg0.toString());
		if (arg0.getColumnValue("Dog_id").equals("4334y-45rtg-435"))
			return true;

		return false;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public Iterable<RowKey> getRowKeys() {
		// TODO Auto-generated method stub
		Iterable<RowKey> i = new Iterable<RowKey>() {
			
			@Override
			public Iterator<RowKey> iterator() {
				
				return new Iterator<RowKey>() {
					int count = 0;
					@Override
					public boolean hasNext() {
						if(count++ == 0){
						return true;
						}
						
						return false;
					}

					@Override
					public RowKey next() {
						
						return new RowKey(columnNames, columnValues1);
					}
				};
			}
		};
		return i;
	}

}
