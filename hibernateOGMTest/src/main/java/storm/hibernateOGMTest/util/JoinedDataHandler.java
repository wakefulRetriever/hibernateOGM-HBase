package storm.hibernateOGMTest.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;

public class JoinedDataHandler {
	private String tableName;
	private String rowkey;
	private Map<String, Map<String, String>> familys = new HashMap<>();
	private Set<String> familyNames;
	private boolean valid;

	public JoinedDataHandler(Set<String> familyNames) {
		this.familyNames = familyNames;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public Map<String, Map<String, String>> getFamilys() {
		return familys;
	}

	public void setFamilys(Map<String, Map<String, String>> familys) {
		this.familys = familys;
	}

	public Set<String> getFamilyNames() {
		return familyNames;
	}

	public void setFamilyNames(Set<String> familyNames) {
		this.familyNames = familyNames;
	}

	public void addData(String familyName, String key, String value) {
		Map<String, String> family = this.familys.get(familyName);
		if (family == null) {
			family = new HashMap<String, String>();
			this.familys.put(familyName, family);
		}
		family.put(key, value);
	}

	public boolean isCompleted() {
		if (familys.keySet().equals(familyNames))
			return true;
		return false;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String ret = "rowkey:" + this.rowkey + "\n";
		ret += this.familys.toString();
		return ret;
	}

	public Put transfromToHBasePut() {
		Put put = new Put(this.rowkey.getBytes());
		for (String family : this.familys.keySet()) {
			Map<String, String> keyvalues = this.familys.get(family);
			for (String key : keyvalues.keySet())
				put.add(family.getBytes(), key.getBytes(), keyvalues.get(key).getBytes());
		}
		return put;
	}

}
