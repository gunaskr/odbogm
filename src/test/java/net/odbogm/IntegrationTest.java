package net.odbogm;

import java.sql.SQLException;
import java.util.logging.Level;

import javax.sql.DataSource;

import org.junit.Before;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public abstract class IntegrationTest {
	protected SessionManager sm;
	
	@Before
    public void setUp() throws SQLException {
        sm = new SessionManager(getDataSource());
        sm.begin();
        System.out.println(sm);
    }
	
	private DataSource getDataSource() {
		HikariConfig config = new HikariConfig();
		config.setDriverClassName("com.orientechnologies.orient.jdbc.OrientJdbcDriver");
	    config.setJdbcUrl("jdbc:orient:remote:localhost/test-ogm");
	    config.setUsername("admin");
	    config.setPassword("admin");
	    config.addDataSourceProperty("cachePrepStmts", "true");
	    config.addDataSourceProperty("prepStmtCacheSize", "250");
	    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
	    config.addDataSourceProperty("allowMultiQueries", "true");
	    config.setMaximumPoolSize(200); 
		return new HikariDataSource(config);
	}
}
