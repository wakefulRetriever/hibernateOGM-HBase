package storm.hibernateOGMTest.domain;


import javax.persistence.Embeddable;


@Embeddable
public class ColumnFamily2 {
	private String cf2key1;
	private String cf2key2;
	private String cf2key3;

	public ColumnFamily2() {

	}

	public ColumnFamily2(String cf2key1, String cf2key2, String cf2key3) {
		super();

		this.cf2key1 = cf2key1;
		this.cf2key2 = cf2key2;
		this.cf2key3 = cf2key3;
	}

	public String getCf2key1() {
		return cf2key1;
	}

	public void setCf2key1(String cf2key1) {
		this.cf2key1 = cf2key1;
	}

	public String getCf2key2() {
		return cf2key2;
	}

	public void setCf2key2(String cf2key2) {
		this.cf2key2 = cf2key2;
	}

	public String getCf2key3() {
		return cf2key3;
	}

	public void setCf2key3(String cf2key3) {
		this.cf2key3 = cf2key3;
	}

}
