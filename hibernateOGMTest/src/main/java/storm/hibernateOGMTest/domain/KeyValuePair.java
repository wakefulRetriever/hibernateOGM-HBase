package storm.hibernateOGMTest.domain;

import javax.persistence.Embeddable;

@Embeddable
public class KeyValuePair{
	private String key;
	private String value;
	public KeyValuePair(){
		
	}
	public KeyValuePair(String key,String value) {
		// TODO Auto-generated constructor stub
		this.key = key;
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return new String("key:"+key+" value:"+value);
	}
	
	
	
}

