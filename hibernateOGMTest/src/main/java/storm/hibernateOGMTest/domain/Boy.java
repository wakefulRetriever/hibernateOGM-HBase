package storm.hibernateOGMTest.domain;

import javax.persistence.Entity;
import javax.persistence.Id;

import storm.hibernateOGMTest.myannotation.MySql;

@Entity
@MySql
public class Boy {
	@Id
	String id;
	String age;
	String name;
	String adress;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAdress() {
		return adress;
	}
	public void setAdress(String adress) {
		this.adress = adress;
	}
	
	
}
