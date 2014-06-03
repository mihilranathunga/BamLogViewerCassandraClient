package compactor;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.context.CarbonContext;

import compactor.exceptions.CassandraClientException;
import compactor.util.Constant;
import core.clients.authentication.CarbonAuthenticatorClient;
import core.clients.service.CassandraKeyspaceAdminClient;


public class LogCompactor {

	private static final Log log = LogFactory.getLog(LogCompactor.class);

	private static CarbonAuthenticatorClient carbonAuthenticatorClient;
	private static String sessionCookie;
	private static CassandraKeyspaceAdminClient keyspaceClient;

	private static java.sql.Connection con = null;

	private static List<String> getLogClientHostIPs() {

		ArrayList<String> columnFamilyIPs = new ArrayList<String>();

		for (String columnFamily : getLogClientColumnFamilies()) {

			String[] parts = columnFamily.split("_");

			String ip = parts[1] + "." + parts[2] + "." + parts[3] + "." + parts[4];

			log.debug("Host IP of Column Family " + columnFamily + " : " + ip);

			columnFamilyIPs.add(ip);

		}
		return columnFamilyIPs;
	}

	private static List<String> getLogClientColumnFamilies() {

		ArrayList<String> logClientColFamilies = new ArrayList<String>();

		String regex =
		               "^" + Constant.LOG_CLIENT_COL_FAMILY_IDENTIFIER +
		                       "_(?:\\d{1,3}_){3}\\d{1,3}";

		for (String columnFamily : getColumnFamilies()) {

			if (columnFamily.matches(regex)) {
				logClientColFamilies.add(columnFamily);
				log.debug(columnFamily + "matches regex of log client CF's");

			}
		}
		return logClientColFamilies;
	}

	private static String[] getColumnFamilies() {

		setKeyStoreProperties();

		Map<String, String> carbonDetails = XMLReader.getAdminUserAndPassword();

		String hostName = Constant.BAM_HOST_NAME;

		String[] columnFamilies = null;

		try {
			carbonAuthenticatorClient = new CarbonAuthenticatorClient(hostName);
			sessionCookie =
			                carbonAuthenticatorClient.login(carbonDetails.get("username"),
			                                                carbonDetails.get("password"), hostName);
			if (sessionCookie.equals(null)) {
				throw new LoginAuthenticationExceptionException("Session Cookie not recieved");
			}
			log.debug("Session Cookie : " + sessionCookie);
			keyspaceClient = new CassandraKeyspaceAdminClient(hostName, sessionCookie);

			columnFamilies =
			                 keyspaceClient.ListColumnFamiliesOfCurrentUser(Constant.CASSANDRA_KS_NAME);

		} catch (AxisFault axisFault) {
			log.error("Axis fault Occured: Getting column families-", axisFault);
		} catch (RemoteException re) {
			log.error("Remote Exception Occured: Getting column families-" + re.getMessage(), re);
		} catch (LoginAuthenticationExceptionException lae) {
			log.error("Login Authentication Exception Occured: Getting column families-" +
			                  lae.getMessage(), lae);

		} /*catch (CassandraKeyspaceAdminCassandraServerManagementException e) {
			log.error("CassandraKeyspaceAdminCassandraServerManagementException occured: Getting column families-" +
			                  e.getMessage(), e);
			e.printStackTrace();
		} */catch (Exception e) {
			log.error("Exception Occured: Getting column families-" + e.getMessage(), e);
		}
		return columnFamilies;

	}

	public static void setKeyStoreProperties() {
		System.setProperty("javax.net.ssl.trustStore", Constant.TRUST_STORE_PATH);
		System.setProperty("javax.net.ssl.trustStorePassword", XMLReader.getTrustStorePassword());
	}

	public static String getData(String hostip, String filekey, String fromTime, String toTime,
	                             int limit) {
		
		String jsonText = null;

		try {
	        try {
	        	con = CassandraCaller.getConnection();
	        } catch (ClassNotFoundException | SQLException e1) {
	        	log.error("Exception Occured: While Connecting to Cassandra-" + e1.getMessage(), e1);
	        	throw new CassandraClientException(e1.getMessage(), e1);
	        }
	        try {
	        	JSONArray result = new JSONArray();

	        	result = CassandraCaller.getLogData(con, hostip, filekey, fromTime, toTime, limit);

	        	StringWriter out = new StringWriter();

	        	result.writeJSONString(out);

	        	jsonText = out.toString();

	        	

	        } catch (IOException | NullPointerException | SQLException e) {
	        	log.error("Exception Occured getting Host ip list-" + e.getMessage(), e);
	        	throw new CassandraClientException(e.getMessage(), e);
	        }
        } catch (CassandraClientException e) {
	        log.error("CassandraClientException Occured getting Data"+ e.getMessage(),e);
        } catch (Exception e){
        	 log.error("Exception Occured getting Data"+ e.getMessage(),e);
        }
		return jsonText;
	}

	@SuppressWarnings("unchecked")
	public static String getIpList() {
		
		String jsonText = null;

		try {
	        try {
	        	JSONArray resultJson = new JSONArray();

	        	for (String ip : getLogClientHostIPs()) {

	        		JSONObject obj = new JSONObject();
	        		obj.put("host_ip", ip);

	        		resultJson.add(obj);
	        	}

	        	StringWriter out = new StringWriter();
	        	resultJson.writeJSONString(out);

	        	jsonText = out.toString();

	        	
	        } catch (IOException e ) {
	        	
	        	throw new CassandraClientException(e.getMessage(), e);
	        }catch(NullPointerException e1){
	        	log.error("Exception Occured getting Host ip list-" + e1.getMessage());
	        	return "{'info':'No hosts yet'}";
	        }
        } catch (CassandraClientException e) {
        	 log.error("CassandraClientException Occured getting ip list"+ e.getMessage(),e);
        } catch (Exception e){
        	log.error("Exception Occured getting ip list"+ e.getMessage(),e);
        }
		return jsonText;

	}

	@SuppressWarnings("unchecked")
	public static String getFileKeyList(String hostip)  {
		
		String jsonText = null;

		try {
	        try {
	        	con = CassandraCaller.getConnection();
	        } catch (ClassNotFoundException | SQLException e1) {
	        	log.error("Exception Occured: While Connecting to Cassandra-" + e1.getMessage(), e1);
	        	throw new CassandraClientException(e1.getMessage(), e1);
	        }

	        ArrayList<String> filekeys = new ArrayList<String>();
	        ResultSet result = null;
	        JSONArray resultJson = new JSONArray();

	        StringWriter out = null;

	        try {
	        	result = CassandraCaller.getFileKeys(con, hostip);

	        	while (result.next()) {

	        		for (int j = 1; j < result.getMetaData().getColumnCount() + 1; j++) {

	        			if (result.getMetaData().getColumnName(j).equals("payload_filekey")) {
	        				String filekey = result.getString(result.getMetaData().getColumnName(j));

	        				if (!filekeys.contains(filekey)) {
	        					filekeys.add(filekey);

	        				}
	        			}
	        		}
	        	}
	        	for (String fkey : filekeys) {
	        		JSONObject obj = new JSONObject();
	        		obj.put("filekey", fkey);
	        		resultJson.add(obj);
	        	}

	        	out = new StringWriter();

	        	resultJson.writeJSONString(out);

	        	jsonText = out.toString();

	        	
	        } catch (SQLException | IOException e) {
	        	log.error("Exception Occured getting filekeys from cassandra -" + e.getMessage(), e);
	        	throw new CassandraClientException(e.getMessage(), e);
	        }
        } catch (CassandraClientException e) {
        	 log.error("CassandraClientException Occured getting file key list"+ e.getMessage(),e);
        }
		catch(Exception e){
			 log.error("CassandraClientException Occured getting file key list"+ e.getMessage(),e);
		}
		return jsonText;
	}

	public static void main(String[] args) {

		PropertyConfigurator.configure("D:" + File.separator + "cloud" + File.separator +
		                               "BamLogViewerCassandraClient" + File.separator +
		                               "resources" + File.separator + "log4j.properties");

		try {
			LogCompactor.setKeyStoreProperties();
			String ips = LogCompactor.getIpList();

			JSONParser parser = new JSONParser();

			Object obj = parser.parse(ips);
			JSONArray array = (JSONArray) obj;

			for (Object hostip : array) {
				System.out.println(((JSONObject) hostip).get("host_ip"));
				LogCompactor.getFileKeyList((String) ((JSONObject) hostip).get("host_ip"));
			}
			LogCompactor.getData("192.168.43.147", "wso2islog", "1401208825432", "1401208825457",
			                     100);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
