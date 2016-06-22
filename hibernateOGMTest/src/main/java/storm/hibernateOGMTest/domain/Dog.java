package storm.hibernateOGMTest.domain;

import javax.persistence.CascadeType;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.TableGenerator;
import java.util.*;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.GenericGenerator;

@Entity
/* @HBase */
public class Dog {
	/**
	 * 标识符
	 */
	@Id
	private String id;
	/**
	 * 列族1
	 */
	@Embedded
	private ColumnFaimly1 cf1;
	/**
	 * 列族2
	 */
	@Embedded
	private ColumnFamily2 cf2;
	/**
	 * 可变列族3
	 */
	@ElementCollection
	private List<KeyValuePair> bodyProperty = new ArrayList<>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ColumnFaimly1 getCf1() {
		return cf1;
	}

	public void setCf1(ColumnFaimly1 cf1) {
		this.cf1 = cf1;
	}

	public ColumnFamily2 getCf2() {
		return cf2;
	}

	public void setCf2(ColumnFamily2 cf2) {
		this.cf2 = cf2;
	}

	public List<KeyValuePair> getBodyProperty() {
		return bodyProperty;
	}

	public void setBodyProperty(List<KeyValuePair> bodyProperty) {
		this.bodyProperty = bodyProperty;
	}

}