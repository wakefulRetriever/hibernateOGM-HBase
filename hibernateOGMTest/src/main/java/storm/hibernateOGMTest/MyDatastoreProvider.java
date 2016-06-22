package storm.hibernateOGMTest;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.exception.spi.Configurable;
import org.hibernate.ogm.datastore.infinispan.InfinispanDialect;
import org.hibernate.ogm.datastore.infinispan.impl.InfinispanDatastoreProvider;
import org.hibernate.ogm.datastore.infinispan.persistencestrategy.impl.KeyProvider;
import org.hibernate.ogm.datastore.infinispan.persistencestrategy.impl.LocalCacheManager;
import org.hibernate.ogm.datastore.keyvalue.options.CacheMappingType;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

public class MyDatastoreProvider extends BaseDatastoreProvider
		implements Startable, Stoppable, ServiceRegistryAwareService, Configurable {

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		// TODO Auto-generated method stub
		return MyGridDialect.class;
	}

	@Override
	public void configure(Properties properties) throws HibernateException {
		// TODO Auto-generated method stub
		System.out.print("configure");
		for (Object o : properties.keySet()) {
			System.out.println(o.toString() + " " + properties.get(o));
		}

		System.out.println(properties.toString());

	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		// TODO Auto-generated method stub
		System.out.println("injectServices");

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		System.out.println("start");

	}

}
