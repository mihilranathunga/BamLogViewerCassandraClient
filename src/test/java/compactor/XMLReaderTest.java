package compactor;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class XMLReaderTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		/*
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		 
        URL[] urls = ((URLClassLoader)cl).getURLs();
 
        for(URL url: urls){
        	System.out.println(url.getFile());
        }*/
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testGetAdminUserAndPassword() {
		Map<String,String> credentials = XMLReader.getAdminUserAndPassword();
	}

	@Test
	public final void testGetCassandraDetails() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGetTrustStorePassword() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testMain() {
		fail("Not yet implemented"); // TODO
	}

}
