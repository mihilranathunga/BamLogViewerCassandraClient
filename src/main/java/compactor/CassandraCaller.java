package compactor;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import compactor.XMLReader;
import compactor.util.Constant;
import compactor.exceptions.CassandraClientException;

public class CassandraCaller {
	
	private static final Log log = LogFactory.getLog(CassandraCaller.class);

	private static java.sql.Connection con = null;


	@SuppressWarnings("unchecked")
	public static JSONArray getLogData(String hostip, String Filekey, String fromTime, String toTime, int limit) throws SQLException, CassandraClientException {

		String logQuery = null;
		String filekeyQuery = "";
		boolean hasFilekey= false;

		
		
		// create indexes before querying
		
		createIndexes(getLogFamilyName(hostip));
		
		if(!Filekey.equals("")){
			//file key given
			filekeyQuery = " WHERE 'payload_filekey'=? AND 'Timestamp' > ? AND 'Timestamp' < ? ";
			hasFilekey = true;
		}
		else{
			filekeyQuery = "";
		}
	
	
		if(hostip!= null && InetAddressValidator.getInstance().isValidInet4Address(hostip)){
			//valid ip

				if(limit > 0 ){
				logQuery =
		                "select 'Timestamp',payload_host_address,payload_filekey,payload_log,payload_timestamp from " +
		                        getLogFamilyName(hostip)+filekeyQuery+" Limit " +
		                        limit;
				}
				else{
					
					logQuery =
							"select 'Timestamp',payload_host_address,payload_filekey,payload_log,payload_timestamp from " +
			                        getLogFamilyName(hostip)+filekeyQuery+" Limit " +
			                        Constant.DEFAULT_RESPONSE_LIMIT;
					
				}
		
				
				
		}
		else{
			throw new CassandraClientException("Host ip is not an valid address or is empty!");
		}

		PreparedStatement statement = null;
		ResultSet rs = null;
		JSONArray resultJson = new JSONArray();
		
			statement = con.prepareStatement(logQuery);
			
			if(hasFilekey){
				
				statement.setString(1, Filekey);
				statement.setLong(2, Long.parseLong(fromTime));
				statement.setLong(3, Long.parseLong(toTime));			
			}

			rs = statement.executeQuery();

			

			while (rs.next()) {
				JSONObject obj = new JSONObject();

				for (int j = 1; j < rs.getMetaData().getColumnCount() + 1; j++) {
					
					if (rs.getMetaData().getColumnName(j).equals("Timestamp")) {
						obj.put("timestamp", rs.getString(rs.getMetaData().getColumnName(j)));

					} 
					else if (rs.getMetaData().getColumnName(j).equals("payload_filekey")) {
						obj.put("filekey", rs.getString(rs.getMetaData().getColumnName(j)));

					} else if (rs.getMetaData().getColumnName(j).equals("payload_host_address")) {
						obj.put("host_ip", rs.getString(rs.getMetaData().getColumnName(j)));

					} else if (rs.getMetaData().getColumnName(j).equals("payload_log")) {
						obj.put("log", rs.getString(rs.getMetaData().getColumnName(j)));

					} else if (rs.getMetaData().getColumnName(j).equals("payload_timestamp")) {

						obj.put("time", rs.getString(rs.getMetaData().getColumnName(j)));

					} else {
						throw new CassandraClientException("Didn't match any result json key types!");
					}
				}

				resultJson.add(obj);

			}
			log.debug("Data Query result :"+resultJson);
			statement.close();

		return resultJson;
	}
	public static ResultSet getFileKeys(String ip) throws SQLException {
		String colFamilyName = getLogFamilyName(ip);
		
		String query =  "select payload_filekey from " +
				colFamilyName+" Limit " +
                Constant.DEFAULT_RESPONSE_LIMIT;
		
		PreparedStatement statement = null;
		ResultSet rs = null;
			
			statement = con.prepareStatement(query);
	        rs = statement.executeQuery();
		
		return rs;
		
	}
	public static String getLogFamilyName(String ip){
		
		
		return Constant.LOG_CLIENT_COL_FAMILY_IDENTIFIER+"_"+ip.replace('.', '_');
	}

	public static void createIndexes(String colFamilyName) {
		String createTimestampIndex = "CREATE INDEX ON " + colFamilyName + "('Timestamp') ";
		String createFileKeyIndex = "CREATE INDEX ON " + colFamilyName + "('payload_filekey') ;";

		String[] indexKeys = { createTimestampIndex, createFileKeyIndex };

		for (int i = 0; i < 2; i++) {


				try {
	                PreparedStatement st = con.prepareStatement(indexKeys[i]);
	                st.executeQuery();
                } catch (SQLException e) {
	                log.error("SQLException Occured Creating indexes -"+e.getMessage(),e);
                }


		}

	}

	public static void getConnection() throws SQLException, ClassNotFoundException {
		Map<String, String> cassandraDetails = XMLReader.getCassandraDetails();
		log.info("\""+(String)cassandraDetails.get("CASSANDRA_JDBC_URL")+"\" , "+(String)cassandraDetails.get("CASSANDRA_USERNAME")+" , "+(String)cassandraDetails.get("CASSANDRA_PASSWORD"));
			con =
			      DriverManager.getConnection((String)cassandraDetails.get("CASSANDRA_JDBC_URL"),
			                                  (String)cassandraDetails.get("CASSANDRA_USERNAME"),
			                                  (String)cassandraDetails.get("CASSANDRA_PASSWORD"));

	}

}