package storm.hibernateOGMTest.DBHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

/**
 * @author storm NoSQL数据库的配置层，将从WEB-INF/ogm.property读取配置信息
 */
abstract public class AbstractConfiguredNoSQLHbaseBase implements NoSQLHBaseBase {
	public static final String DEFAULT_HOST = "10.108.209.95";
	public static final String OGM_PROPERTIES_PATH = "ogm.properties";
	private Properties properties;
	private Configuration hbaseConfiguration = null;

	public AbstractConfiguredNoSQLHbaseBase() {
		// TODO Auto-generated constructor stub
		initProperty();
		configureAndInit();
	}

	public Configuration getHbaseConfiguration() {
		return hbaseConfiguration;
	}

	public void setHbaseConfiguration(Configuration hbaseConfiguration) {
		this.hbaseConfiguration = hbaseConfiguration;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void configureAndInit() {
		// TODO Auto-generated method stub
		Map<String, String> configure = new HashMap<>();
		configure.put("host", properties.getProperty("hbaseHost"));
		System.out.println(configure.get("host"));
		configureAndInit(configure);
	}

	@Override
	public void configureAndInit(Map<String, String> configures) {
		// TODO Auto-generated method stub

		String ip = configures.get("host");
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ip);
		// 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConfiguration = HBaseConfiguration.create(conf);
	}

	private void initProperty() {
		properties = new Properties();
		FileInputStream inputStream = null;
		try {
			// inputStream = new FileInputStream(new File(OGM_PROPERTIES_PATH));
			properties.load(this.getClass().getResourceAsStream("/" + OGM_PROPERTIES_PATH));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("ogm.property is not exist!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("property read exception!");
		} finally {
			if (inputStream != null)
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	}
	

}
