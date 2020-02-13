package edu.ucdavis.ucdh.stu.snutil.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;


/**
 * <p>Copies an HM SM Incident over to ServiceNow.</p>
 */
public class CopyIncident implements SpringBatchJob {
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String INCIDENT_SYSID_URL = "/api/now/table/incident?sysparm_fields=sys_id&sysparm_query=number%3D";
	private static final String INCIDENT_URL = "/api/now/table/incident";
	private final Log log = LogFactory.getLog(getClass().getName());
	private Map<String,String> userSysId = new HashMap<String,String>();
	private HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
	private Connection conn = null;
	private String id = null;
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;
	private DataSource dataSource = null;
	private int smRecordsRead = 0;
	private int snRecordsInserted = 0;
	private int snRecordsUpdated = 0;
	private int smRecordsUpdated = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int jobId) throws Exception {
		log.info("CopyIncident starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify incident id
		if (args.length > 1) {
			id = args[1];
		}
		if (StringUtils.isEmpty(id)) {
			throw new IllegalArgumentException("Required argument \"id\" missing or invalid.");
		} else {
			log.info("HP SM Incident ID = " + id);
		}
		// verify serviceNowServer
		if (StringUtils.isEmpty(serviceNowServer)) {
			throw new IllegalArgumentException("Required property \"serviceNowServer\" missing or invalid.");
		} else {
			log.info("serviceNowServer = " + serviceNowServer);
		}
		// verify serviceNowUser
		if (StringUtils.isEmpty(serviceNowUser)) {
			throw new IllegalArgumentException("Required property \"serviceNowUser\" missing or invalid.");
		} else {
			log.info("serviceNowUser = " + serviceNowUser);
		}
		// verify serviceNowPassword
		if (StringUtils.isEmpty(serviceNowPassword)) {
			throw new IllegalArgumentException("Required property \"serviceNowPassword\" missing or invalid.");
		} else {
			log.info("serviceNowPassword = **********");
		}
		// verify dataSource
		if (dataSource == null) {
			throw new IllegalArgumentException("Required property \"prDataSource\" missing or invalid.");
		} else {
			try {
				conn = dataSource.getConnection();
				log.info("Connection established to dataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to dataSource: " + e, e);
			}
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");


		JSONObject incident = fetchIncident();
		if (incident == null) {
			throw new IllegalArgumentException("There is no Incident on file with an ID of " + id);
		} else {
			log.info("Incident " + id + " retrieved from HP SM database.");
		}
		// verify serviceNowServer
		String serviceNowId = (String) incident.get("number");
		if (StringUtils.isEmpty(serviceNowId)) {
			log.info("No ServiceNow ID present in HP SM data -- Inserting new ServiceNow Incident.");
			insertIncident(incident);
		} else {
			log.info("Updating ServiceNow Incident " + serviceNowId);
			updateIncident(incident);
		}

		// close database connection
		conn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (smRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("HP SM Incident records read", BatchJobService.FORMAT_INTEGER, new BigInteger(smRecordsRead + "")));
		}
		if (snRecordsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Incident records inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(snRecordsInserted + "")));
		}
		if (snRecordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Incident records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(snRecordsUpdated + "")));
		}
		if (smRecordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("HP SM Incident records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(smRecordsUpdated + "")));
		}

		// end job
		log.info(" ");
		log.info("CopyIncident complete.");

		return stats;
	}

	@SuppressWarnings("unchecked")
	private JSONObject fetchIncident() {
		JSONObject incident = null;

		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "SELECT a.TITLE, a.DESCRIPTION, c.UCD_IAM_ID AS CONTACT, a.LOCATION, a.LOGICAL_NAME, a.[OPEN], CASE WHEN a.ACTIVE='f' THEN 'false' ELSE 'true' END AS ACTIVE, a.CATEGORY, a.SUBCATEGORY, a.INITIAL_IMPACT, a.SEVERITY, a.PRIORITY_CODE, a.OPEN_TIME, e.UCD_IAM_ID AS OPENED_BY, a.CLOSE_TIME, d.UCD_IAM_ID AS CLOSED_BY, a.RESOLUTION_CODE, a.RESOLUTION, b.UCD_SERVICENOW_ID FROM INCIDENTSM1 a LEFT OUTER JOIN INCIDENTSM2 b ON b.INCIDENT_ID=a.INCIDENT_ID LEFT OUTER JOIN CONTCTSM1 c ON c.CONTACT_NAME=a.CONTACT_NAME LEFT OUTER JOIN CONTCTSM1 d ON d.CONTACT_NAME=a.CLOSED_BY LEFT OUTER JOIN CONTCTSM1 e ON e.CONTACT_NAME=a.OPENED_BY WHERE a.INCIDENT_ID=?";
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				smRecordsRead++;
				if (log.isDebugEnabled()) {
					log.debug("HP SM Incident " + id + " found.");
				}
				incident = new JSONObject();
				incident.put("u_hp_sm_id", id);
				incident.put("sys_domain", "global");
				incident.put("company", "UC Davis Health");
				incident.put("short_description", rs.getString("TITLE"));
				incident.put("description", rs.getString("DESCRIPTION"));
				incident.put("caller_id",  getUserSysId(rs.getString("CONTACT")));
				incident.put("location", rs.getString("LOCATION"));
				incident.put("cmdb_ci", rs.getString("LOGICAL_NAME"));
				incident.put("state", rs.getString("OPEN"));
				incident.put("incident_state", rs.getString("OPEN"));
				incident.put("active", rs.getString("ACTIVE"));
				incident.put("category", rs.getString("CATEGORY"));
				incident.put("subcategory", rs.getString("SUBCATEGORY"));
				incident.put("impact", rs.getString("INITIAL_IMPACT"));
				incident.put("severity", rs.getString("SEVERITY"));
				incident.put("priority", rs.getString("PRIORITY_CODE"));
				incident.put("opened_at", rs.getString("OPEN_TIME"));
				incident.put("opened_at", rs.getString("OPEN_TIME"));
				incident.put("opened_by",  getUserSysId(rs.getString("OPENED_BY")));
				incident.put("expected_start", rs.getString("OPEN_TIME"));
				incident.put("closed_at", rs.getString("CLOSE_TIME"));
				incident.put("resolved_at", rs.getString("CLOSE_TIME"));
				incident.put("work_end", rs.getString("CLOSE_TIME"));
				incident.put("closed_by", getUserSysId(rs.getString("CLOSED_BY")));
				incident.put("resolved_by",  getUserSysId(rs.getString("CLOSED_BY")));
				incident.put("close_code", rs.getString("RESOLUTION_CODE"));
				incident.put("work_notes", rs.getString("RESOLUTION"));
				incident.put("number", rs.getString("UCD_SERVICENOW_ID"));
				Iterator<String> i = incident.keySet().iterator();
				List<String> nullKey = new ArrayList<String>();
				while (i.hasNext()) {
					String key = i.next();
					if (incident.get(key) == null) {
						nullKey.add(key);
					}
				}
				i = nullKey.iterator();
				while (i.hasNext()) {
					incident.remove(i.next());
				}
			} else {
				log.error("HP SM Incident " + id + " not found.");
			}
		} catch (Exception e) {
			log.error("Exception encountered fetching HP SM Incident " + id + ": " + e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}

		return incident;
	}

	private void insertIncident(JSONObject incident) {
		String url = serviceNowServer + INCIDENT_URL;
		// create HttpPost
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// put JSON
		try {
			post.setEntity(new StringEntity(incident.toJSONString()));
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200 || rc == 201) {
				snRecordsInserted++;
				JSONObject result = (JSONObject) JSONValue.parse(resp);
				if (result != null) {
					if (log.isDebugEnabled()) {
						log.debug("JSON response: " + result.toJSONString());
					}
					result = (JSONObject) result.get("result");
					if (result != null) {
						String serviceNowId = (String) result.get("number");
						if (StringUtils.isNotEmpty(serviceNowId)) {
							if (log.isDebugEnabled()) {
								log.debug("ServiceNow ID: " + serviceNowId);
							}
							updateServiceManager(id, serviceNowId);
						}
					}
				}
			} else {
				log.error("Invalid response from ServiceNow to POST: rc=" + rc + "; response=" + resp);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to insert incident: " + e.toString(), e);
		}
	}

	private void updateIncident(JSONObject incident) {
		String url = serviceNowServer + INCIDENT_URL + "/" + fetchIncidentSysId((String) incident.get("number"));
		// create HttpPut
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// put JSON
		try {
			put.setEntity(new StringEntity(incident.toJSONString()));
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			String responseString = EntityUtils.toString(resp.getEntity());
			if (rc == 200 || rc == 201) {
				snRecordsUpdated++;
				if (log.isDebugEnabled()) {
					log.debug("ServiceNow response to PUT: " + responseString);
				}
			} else {
				log.error("Invalid response from ServiceNow to PUT: rc=" + rc + "; response=" + responseString);
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to insert incident " + incident.get("u_hp_sm_id") + ": " + e, e);
		}
	}

	private void updateServiceManager(String id, String serviceNowId) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "UPDATE INCIDENTSM2 SET UCD_SERVICENOW_ID=? WHERE INCIDENT_ID=?";
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, serviceNowId);
			ps.setString(2, id);
			if (ps.executeUpdate() > 0) {
				smRecordsUpdated++;
				if (log.isDebugEnabled()) {
					log.debug("Incident " + id + " updated with ServiceNow id " + serviceNowId);
				}
			} else {
				log.error("Incident " + id + " was not updated with ServiceNow id " + serviceNowId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}
	}

	private String getUserSysId(String iamId) {
		String sysId = null;

		if (userSysId.containsKey(iamId)) {
			sysId = userSysId.get(iamId);
		} else {
			sysId = fetchUserSysId(iamId);
			if (StringUtils.isNotEmpty(sysId)) {
				 userSysId.put(iamId, sysId);
			}
		}

		return sysId;
	}

	private String fetchUserSysId(String iamId) {
		String sysId = "";

		String url = serviceNowServer + SYSID_URL + iamId;
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200) {
				JSONObject json = (JSONObject) JSONValue.parse(resp);
				if (json != null) {
					JSONArray result = (JSONArray) json.get("result");
					if (result != null && result.size() > 0) {
						JSONObject obj = (JSONObject) result.get(0);
						sysId = (String) obj.get("sys_id");
					}
				}
			} else {
				log.error("Invalid response received from HTTP GET: " + rc);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to fetch user sys_id: " + e.toString(), e);
		}

		return sysId;
	}

	private String fetchIncidentSysId(String serviceNowId) {
		String sysId = "";

		String url = serviceNowServer + INCIDENT_SYSID_URL + serviceNowId;
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200) {
				JSONObject json = (JSONObject) JSONValue.parse(resp);
				if (json != null) {
					JSONArray result = (JSONArray) json.get("result");
					if (result != null && result.size() > 0) {
						JSONObject obj = (JSONObject) result.get(0);
						sysId = (String) obj.get("sys_id");
					}
				}
			} else {
				log.error("Invalid response received from HTTP GET: " + rc);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to fetch incident sys_id: " + e.toString(), e);
		}

		return sysId;
	}

	/**
	 * @param serviceNowServer the serviceNowServer to set
	 */
	public void setServiceNowServer(String serviceNowServer) {
		this.serviceNowServer = serviceNowServer;
	}

	/**
	 * @param serviceNowUser the serviceNowUser to set
	 */
	public void setServiceNowUser(String serviceNowUser) {
		this.serviceNowUser = serviceNowUser;
	}

	/**
	 * @param serviceNowPassword the serviceNowPassword to set
	 */
	public void setServiceNowPassword(String serviceNowPassword) {
		this.serviceNowPassword = serviceNowPassword;
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
}
