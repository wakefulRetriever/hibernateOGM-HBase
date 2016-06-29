package storm.hibernateOGMTest;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.transaction.TransactionManager;

import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

import storm.hibernateOGMTest.domain.Boy;
import storm.hibernateOGMTest.domain.Breed;
import storm.hibernateOGMTest.domain.ColumnFaimly1;
import storm.hibernateOGMTest.domain.ColumnFamily2;
import storm.hibernateOGMTest.domain.Dog;

import java.lang.reflect.Field;
import java.util.*;
import storm.hibernateOGMTest.domain.KeyValuePair;
/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		
		TransactionManager tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
		//final Logger logger = LoggerFactory.(App.class);
		// build the EntityManagerFactory as you would build in in Hibernate
		// Core
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("ogm-jpa-tutorial");

		// Persist entities the way you are used to in plain JPA
		try {
			tm.begin();
			// logger.infof( "About to store dog and breed" );
			EntityManager em = emf.createEntityManager();
			//mysqlInsertTest(em);
			em.createNamedQuery("select * from Boy;").getMaxResults();
			//mysqlQueryTest(em);
			
			//em.flush();
			//queryTest(em);
			em.close();
			tm.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//reflectTest();
	}
	private static void createTest(EntityManager em){
		//em.persist(collie);
		Dog dina = new Dog();
		dina.setId("12-wer-sdfs");
		dina.setCf1(new ColumnFaimly1("cf1value1","cf1value2", "cf1value3"));
		dina.setCf2(new ColumnFamily2("cf2value1","cf2value2", "cf2value3"));
		
		List<KeyValuePair> list2 = new ArrayList<>();
		list2.add(new KeyValuePair("key21","valxue21"));
		list2.add(new KeyValuePair("key22","value22"));
		dina.setBodyProperty(list2);

		em.persist(dina);
		
	}
	private static void queryTest(EntityManager em){
		Dog b = em.find(Dog.class, "4334y-45rtg-43ooo");
		//em.createNativeQuery("select * from Dog;").getResultList();
		
		if(b == null)
			System.out.println("dog is null");
		else 
			System.out.println(b.getCf1().getCf1key1());
	}
	public static void mysqlInsertTest(EntityManager em){
		Boy b = new Boy();
		b.setAdress("北京邮电大学");
		b.setAge("18");
		b.setId("fsllddd-ddd");
		b.setName("boyboy");
		em.persist(b);
		
	}
	public static void mysqlQueryTest(EntityManager em){
	System.out.println("mysqlQueryTest start...");
		Boy b = em.find(Boy.class, "fsllddd-ddd");
		if(b == null) System.out.println("b == null");
		System.out.println("-----------" + b.getAdress() + "------------");
		
	}
	
	public static void reflectTest(){
		try {
			Class<?> clazz = Class.forName("storm.hibernateOGMTest.domain.Dog");
			
			for(Field f:clazz.getDeclaredFields()){
				System.out.println(f.getName());
			};
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
