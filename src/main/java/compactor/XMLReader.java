package compactor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import compactor.util.Constant;

public class XMLReader {

	private static final Log log = LogFactory.getLog(XMLReader.class);


	public static Map<String, String> getAdminUserAndPassword() {
		Map<String, String> adminUserDetails = new HashMap<String, String>();;
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader reader = null;
		String adminUsername = null;
		String adminPassword = null;
		try {
			reader = factory.createXMLStreamReader(new FileReader(Constant.USER_MGT_XML_PATH));

			String tagContent = null;

			while (reader.hasNext()) {
				int event = reader.next();

				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						break;

					case XMLStreamConstants.CHARACTERS:
						tagContent = reader.getText().trim();
						break;

					case XMLStreamConstants.END_ELEMENT:
						switch (reader.getLocalName()) {
							case "UserName":
								adminUserDetails.put("username", tagContent);
								adminUsername = tagContent;
								break;
							case "Password":
								adminUserDetails.put("password", tagContent);
								adminPassword = tagContent;
								break;
						}
						break;

					case XMLStreamConstants.START_DOCUMENT:

						break;
				}

			}
		} catch (XMLStreamException | FileNotFoundException e) {
			log.error("Error Reading USR_MGT_XML ", e);
		}

		log.info("Admin userName : " + adminUsername);
		log.info("Admin Password : " + adminPassword);
		return adminUserDetails;

	}

	public static Map<String, String> getCassandraDetails() {
		Map<String, String> cassandraDetails = new HashMap<String, String>();
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader reader = null;
		
		 String CASSANDRA_JDBC_URL = null;
		 String CASSANDRA_USERNAME = null;
		 String CASSANDRA_PASSWORD = null;
		 String CASSANDRA_HOST_NAME = null;
		 String CASSANDRA_KS_NAME = null;
		 
		String datasourceName = null;
		
		try {
			reader =
			         factory.createXMLStreamReader(new FileReader(
			                                                      Constant.MASTER_DATASOURCES_XML_PATH));

			String tagContent = null;

			while (reader.hasNext()) {
				int event = reader.next();

				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						break;

					case XMLStreamConstants.CHARACTERS:
						tagContent = reader.getText().trim();
						break;

					case XMLStreamConstants.END_ELEMENT:
						switch (reader.getLocalName()) {
							case "name":
								datasourceName = tagContent;
								break;
							case "url":
								if (datasourceName.equals("WSO2BAM_CASSANDRA_DATASOURCE")) {
									cassandraDetails.put("CASSANDRA_JDBC_URL", tagContent);
									CASSANDRA_JDBC_URL = tagContent;
									CASSANDRA_HOST_NAME = CASSANDRA_JDBC_URL.split("/+")[1];
									cassandraDetails.put("CASSANDRA_HOST_NAME", CASSANDRA_HOST_NAME);
									CASSANDRA_KS_NAME = CASSANDRA_JDBC_URL.split("/+")[2];
									cassandraDetails.put("CASSANDRA_KS_NAME", CASSANDRA_KS_NAME);
								}
								break;
							case "username":
								if (datasourceName.equals("WSO2BAM_CASSANDRA_DATASOURCE")) {
									cassandraDetails.put("CASSANDRA_USERNAME", tagContent);
									CASSANDRA_USERNAME = tagContent;
								}
								break;
							case "password":
								if (datasourceName.equals("WSO2BAM_CASSANDRA_DATASOURCE")) {
									cassandraDetails.put("CASSANDRA_PASSWORD", tagContent);
									CASSANDRA_PASSWORD = tagContent;
								}
								break;
						}
						break;

					case XMLStreamConstants.START_DOCUMENT:

						break;
				}

			}
		} catch (XMLStreamException | FileNotFoundException e) {
			log.error("Error Reading MASTER_DATASOURCES_XML ", e);
		}

		log.info("Cassandra JDBC Url : " + CASSANDRA_JDBC_URL);
		log.info("Cassandra Admin User Name : " + CASSANDRA_USERNAME);
		log.info("Cassandra Admin Password : " + CASSANDRA_PASSWORD);
		log.info("Cassandra Host Name : "+CASSANDRA_HOST_NAME);
		log.info("Cassandra Default KS Name : "+CASSANDRA_KS_NAME);
		return cassandraDetails;

	}
	public static String getTrustStorePassword() {

		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader reader = null;
		String TRUST_STORE_PASSWORD = "wso2carbon";

		boolean trustStore = false;
		try {
			reader =
			         factory.createXMLStreamReader(new FileReader(
			                                                      Constant.CARBON_XML_PATH));

			String tagContent = null;

			while (reader.hasNext()) {
				int event = reader.next();

				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						if ("TrustStore".equals(reader.getLocalName())) {
							trustStore = true;
						}
						break;

					case XMLStreamConstants.CHARACTERS:
						tagContent = reader.getText().trim();
						break;

					case XMLStreamConstants.END_ELEMENT:
						switch (reader.getLocalName()) {
							case "Password":
								if (trustStore) {
									TRUST_STORE_PASSWORD = tagContent;
								}
								break;
							case "TrustStore":
								if (trustStore) {
									trustStore = false;
								}
								break;
						}
						break;

					case XMLStreamConstants.START_DOCUMENT:

						break;
				}

			}
		} catch (XMLStreamException | FileNotFoundException e) {
			log.error("Error Reading CARBON_XML ", e);
		}

		log.info("Trust Store Password : "+TRUST_STORE_PASSWORD);
		return TRUST_STORE_PASSWORD;

	}
	

}
