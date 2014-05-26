/**
 * 
 */
package compactor.util;

import java.io.File;

import org.wso2.carbon.utils.CarbonUtils;

/**
 * @author mihil
 *
 */
public class Constant {
	
	public static final String BAM_HOST_NAME = "localhost:9443";
	
	public static final String TRUST_STORE_PATH = CarbonUtils.getCarbonHome()+ File.separator +
							"repository" + File.separator + "resources" + File.separator + "security" + File.separator + "client-truststore.jks";
	
	public static String USER_MGT_XML_PATH = CarbonUtils.getCarbonHome() + File.separator +
			"repository" + File.separator + "conf" + File.separator + "user-mgt.xml";
	
	public static String MASTER_DATASOURCES_XML_PATH = CarbonUtils.getCarbonHome() + File.separator +
			"repository" + File.separator + "conf" + File.separator + "datasources" + File.separator +"master-datasources.xml";
	
	public static final String CARBON_XML_PATH = "/home/mihil/WSO2/wso2bam-2.4.0" + File.separator +
			"repository" + File.separator + "conf" + File.separator + "carbon.xml";
	
	public static final String LOG_CLIENT_COL_FAMILY_IDENTIFIER = "logs";
	
	public static final int DEFAULT_RESPONSE_LIMIT = 1000;

	public static final String LOG4J_PROPERTY_PATH =/*"./src/main/resources/log4j.properties";*/CarbonUtils.getCarbonHome()+ File.separator +
			"repository" + File.separator + "components" + File.separator + "lib" + File.separator +"log4j.properties";

	public static final String CASSANDRA_KS_NAME = "EVENT_KS";
	
	

	
	


}
