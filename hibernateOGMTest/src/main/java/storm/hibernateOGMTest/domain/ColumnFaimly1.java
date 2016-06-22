package storm.hibernateOGMTest.domain;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class ColumnFaimly1 {
	private String cf1key1;
	private String cf1key2;
	private String cf1key3;

	
	@Id
	private String id;
	
	public ColumnFaimly1(){
		
	}
	public ColumnFaimly1(String cf1key1, String cf1key2, String cf1key3) {
		super();
		this.cf1key1 = cf1key1;
		this.cf1key2 = cf1key2;
		this.cf1key3 = cf1key3;
	}

	public String getCf1key1() {
		return cf1key1;
	}

	public void setCf1key1(String cf1key1) {
		this.cf1key1 = cf1key1;
	}

	public String getCf1key2() {
		return cf1key2;
	}

	public void setCf1key2(String cf1key2) {
		this.cf1key2 = cf1key2;
	}

	public String getCf1key3() {
		return cf1key3;
	}

	public void setCf1key3(String cf1key3) {
		this.cf1key3 = cf1key3;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
