/**
 * 
 */
package compactor;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Mihil
 *
 */
public class CassandraCallerTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		PropertyConfigurator.configure("D:" + File.separator +"cloud"+ File.separator +"BamLogViewerCassandraClient"+ File.separator + "resources" + File.separator +"log4j.properties");
		
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link compactor.CassandraCaller#getLogData(java.lang.String, java.lang.String, java.lang.String, java.lang.String, int)}.
	 */
	@Test
	public final void testGetLogData() {
		 // TODO
	}

	/**
	 * Test method for {@link compactor.CassandraCaller#getFileKeys(java.lang.String)}.
	 */
	@Test
	public final void testGetFileKeys() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link compactor.CassandraCaller#getLogFamilyName(java.lang.String)}.
	 */
	@Test
	public final void testGetLogFamilyName() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link compactor.CassandraCaller#createIndexes(java.lang.String)}.
	 */
	@Test
	public final void testCreateIndexes() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link compactor.CassandraCaller#getConnection()}.
	 */
	@Test
	public final void testGetConnection() {
		
		fail("Not yet implemented");
	}

}
