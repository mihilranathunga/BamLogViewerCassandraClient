package compactor;

import java.io.IOException;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.cassandra.mgt.stub.ks.CassandraKeyspaceAdminCassandraServerManagementException;

import compactor.exceptions.CassandraClientException;
import compactor.util.Constant;

import core.clients.authentication.CarbonAuthenticatorClient;
import core.clients.service.CassandraKeyspaceAdminClient;

public class LogCompactor {
	
	private static final Log log = LogFactory.getLog(LogCompactor.class);
	
	private CarbonAuthenticatorClient carbonAuthenticatorClient;
	private String sessionCookie;
	private CassandraKeyspaceAdminClient keyspaceClient;
	

	private List<String> getLogClientHostIPs() {

		ArrayList<String> columnFamilyIPs = new ArrayList<String>();

		for (String columnFamily : getLogClientColumnFamilies()) {

			String[] parts = columnFamily.split("_");

			String ip = parts[1] + "." + parts[2] + "." + parts[3] + "." + parts[4];

			log.info("Host IP of Column Family "+columnFamily+" : "+ip);

			columnFamilyIPs.add(ip);

		}
		return columnFamilyIPs;
	}


	private List<String> getLogClientColumnFamilies() {

		ArrayList<String> logClientColFamilies = new ArrayList<String>();

		String regex =
				"^" + Constant.LOG_CLIENT_COL_FAMILY_IDENTIFIER +
				"_(?:\\d{1,3}_){3}\\d{1,3}";

		for (String columnFamily : getColumnFamilies()) {

			if (columnFamily.matches(regex)) {
				logClientColFamilies.add(columnFamily);
				log.info(columnFamily + "matches regex of log client CF's");

			}
		}
		return logClientColFamilies;
	}


	private String[] getColumnFamilies() {

		setKeyStoreProperties();
		
		Map<String, String> carbonDetails = XMLReader.getAdminUserAndPassword();
		
		String hostName = Constant.BAM_HOST_NAME;

		String[] columnFamilies = null;

		try {
			carbonAuthenticatorClient = new CarbonAuthenticatorClient(hostName);
			sessionCookie = carbonAuthenticatorClient.login((String)carbonDetails.get("username"), (String)carbonDetails.get("password"), hostName);
			if (sessionCookie.equals(null)) {
				throw new LoginAuthenticationExceptionException("Session Cookie not recieved");
			}
			log.debug("Session Cookie : "+sessionCookie);
			keyspaceClient = new CassandraKeyspaceAdminClient(hostName, sessionCookie);

			columnFamilies = keyspaceClient.ListColumnFamiliesOfCurrentUser(Constant.CASSANDRA_KS_NAME);

		} catch (AxisFault axisFault) {
			log.error("Axis fault Occured: Getting column families-" ,axisFault);
		} catch (RemoteException re) {
			log.error("Remote Exception Occured: Getting column families-"+re.getMessage(),re);
		} catch (LoginAuthenticationExceptionException lae) {
			log.error("Login Authentication Exception Occured: Getting column families-"+lae.getMessage(),lae);

		} catch (CassandraKeyspaceAdminCassandraServerManagementException e) {
			log.error("CassandraKeyspaceAdminCassandraServerManagementException occured: Getting column families-"+e.getMessage(),e);
			e.printStackTrace();
		}catch (Exception e){
			log.error("Exception Occured: Getting column families-"+e.getMessage(), e);
		}
		return columnFamilies;

	}

	public static void setKeyStoreProperties() {
		System.setProperty("javax.net.ssl.trustStore", Constant.TRUST_STORE_PATH);
		System.setProperty("javax.net.ssl.trustStorePassword", XMLReader.getTrustStorePassword());
	}



	public String getData(String hostip, String filekey, String fromTime, String toTime, int limit) {
		
		try {
	        CassandraCaller.getConnection();
        } catch (ClassNotFoundException e1) {
	        log.error("ClassNotFound Exception Occured: While Connecting to Cassandra-"+e1.getMessage(),e1);
        } catch (SQLException e1) {
        	log.error("SQLException Exception Occured: While Connecting to Cassandra-"+e1.getMessage(),e1);
        }
		
		JSONArray result = new JSONArray();

			try {
	            result = CassandraCaller.getLogData(hostip, filekey, fromTime, toTime, limit);
            } catch (SQLException e1) {
            	 log.error("SQLException Exception Occured: While Getting Data From Cassandra-"+e1.getMessage(),e1);
            } catch (CassandraClientException e1) {
            	 log.error("CassandraClientException Exception Occured: While Getting Data From Cassandra-"+e1.getMessage(),e1);
            }


		StringWriter out = new StringWriter();
		try {
			result.writeJSONString(out);
		} catch (IOException e) {
			log.error("Error Serielizing Data Result-"+e.getMessage(), e);
		}

		String jsonText = out.toString();

		return jsonText;
	}

	@SuppressWarnings("unchecked")
	public String getIpList() {

		JSONArray resultJson = new JSONArray();

		for (String ip : getLogClientHostIPs()) {

			JSONObject obj = new JSONObject();
			obj.put("host_ip", ip);

			resultJson.add(obj);
		}

		StringWriter out = new StringWriter();
		try {
			resultJson.writeJSONString(out);
		} catch (IOException e) {
			log.error("Error Serielizing IP List Result-"+e.getMessage(), e);
		}

		String jsonText = out.toString();

		return jsonText;
	}

	@SuppressWarnings("unchecked")
	public String getFileKeyList(String hostip) {
		
		try {
	        CassandraCaller.getConnection();
        } catch (ClassNotFoundException e1) {
	        log.error("ClassNotFound Exception Occured: While Connecting to Cassandra-"+e1.getMessage(),e1);
        } catch (SQLException e1) {
        	log.error("SQLException Exception Occured: While Connecting to Cassandra-"+e1.getMessage(),e1);
        }

		ArrayList<String> filekeys = new ArrayList<String>();
		ResultSet result = null;
		JSONArray resultJson = new JSONArray();

		StringWriter out = null;
		
		try {
			result = CassandraCaller.getFileKeys(hostip);

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
		} catch (SQLException e ) {
			log.error("SQLException Occured getting filekeys from cassandra -"+ e.getMessage(),e);
		} catch (IOException e){
			log.error("Error Serielizing filekey List Result-"+e.getMessage(), e);
		}

		String jsonText = out.toString();

		return jsonText;

	}
	
	public static void main(String[] args) {

		LogCompactor comp = new LogCompactor();

		try {
			//comp.getCassandraDetails();
			//comp.getTrustStorePassword();
			// comp.getData("192.168.1.103","syslog","1400132343654","1400137741565",10);
			// System.out.println(comp.getIpList());
			//System.out.println(comp.getFileKeyList("192.168.1.103"));
			//XMLReader.getAdminUserAndPassword();
			//getCassandraDetails();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
