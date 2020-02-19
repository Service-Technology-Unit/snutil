package edu.ucdavis.ucdh.stu.snutil.batch;
import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;

/**
 * <p>Copies IT Time Sheets over to ServiceNow.</p>
 */
public class CopyTimeSheet implements SpringBatchJob {
	private static final String LIST_SQL = "SELECT WORKDATE, WORKNAME, SYSMODTIME FROM DAILY_TIME WHERE WORKLINE=0 AND SYSMODTIME>? ORDER BY SYSMODTIME";
	private static final String TIMECARD_SQL = "SELECT DISTINCT WORKITEM, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=?) AS SUNDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,1,?)) AS MONDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,2,?)) AS TUESDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,3,?)) AS WEDNESDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,4,?)) AS THURSDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,5,?)) AS FRIDAY, (SELECT SUM(REG_HOURS) FROM DAILY_TIME WHERE WORKNAME=a.WORKNAME AND WORKITEM=a.WORKITEM AND WORKDATE=DATEADD(DAY,6,?)) AS SATURDAY FROM DAILY_TIME a WHERE WORKNAME=? AND WORKDATE>=? AND WORKDATE<DATEADD(DAY,7,?) AND REG_HOURS>0 ORDER BY WORKITEM";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=user_name%3D";
	private static final String TIME_SHEET_SYSID_URL = "/api/now/table/time_sheet?sysparm_fields=sys_id&sysparm_query=week_starts_on%3D{date}%5Euser%3D";
	private static final String TIME_SHEET_URL = "/api/now/table/time_sheet";
	private static final String TIME_CARD_URL = "/api/now/table/time_card";
	private static final String TIME_CARD_FETCH_URL = "/api/now/table/time_card?sysparm_fields=sys_id&sysparm_query=time_sheet%3D";
	private static final String LAST_UPDATE_FILE_NAME = "conf/synctime/lastupdate.txt";
	private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");
	private static final DateFormat DF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final Log log = LogFactory.getLog(getClass().getName());
	private Map<String,String> userSysId = new HashMap<String,String>();
	private List<TimeSheet> timeSheet = new ArrayList<TimeSheet>();
	private Iterator<TimeSheet> i = null;
	private HttpClient client = null;
	private Connection conn = null;
	private Date lastUpdate = null;
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;
	private DataSource dataSource = null;
	private int tsRecordsRead = 0;
	private int tcRecordsRead = 0;
	private int usersFound = 0;
	private int usersNotFound = 0;
	private int snTimeSheetsFound = 0;
	private int snTimeSheetsNotFound = 0;
	private int snTimeCardsDeleted = 0;
	private int snTimeSheetsInserted = 0;
	private int snTimeCardsInserted = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int jobId) throws Exception {
		copyTimeSheetBegin();
		while (i.hasNext()) {
			person(i.next());
		}
		return copyTimeSheetEnd();
	}

	private void copyTimeSheetBegin() throws Exception {
		log.info("CopyTimeSheet starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

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
			throw new IllegalArgumentException("Required property \"dataSource\" missing or invalid.");
		} else {
			try {
				conn = dataSource.getConnection();
				log.info("Connection established to dataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to dataSource: " + e, e);
			}
		}
		// verify HTTP client
		try {
			client = HttpClientProvider.getClient();
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to create HTTP client: " + e, e);
		}
		// fetch last update date
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(LAST_UPDATE_FILE_NAME));
			lastUpdate = DF2.parse(reader.readLine());
		} catch (Exception e) {
			lastUpdate = DF2.parse("2017-07-01 00:00:00.000");
		} finally {
			try {
				reader.close();
			} catch (Exception e) {
				// no one cares
			}
		}
		log.info("This run will process Time Sheets comepleted after " + DF2.format(lastUpdate));

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");


		PreparedStatement ps = conn.prepareStatement(LIST_SQL);
		ps.setTimestamp(1, new Timestamp(lastUpdate.getTime()));
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			tsRecordsRead++;
			TimeSheet ts = new TimeSheet(rs.getString("WORKNAME"), rs.getDate("WORKDATE"), rs.getTimestamp("SYSMODTIME"));
			timeSheet.add(ts);
		}
		rs.close();
		ps.close();
		i = timeSheet.iterator();
	}

	private List<BatchJobServiceStatistic> copyTimeSheetEnd() throws Exception {
		// close database connection
		conn.close();

		// save last update date
		try {
			Files.write(Paths.get(LAST_UPDATE_FILE_NAME), DF2.format(lastUpdate).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);;
			log.info("Last update date saved for next run: " + DF2.format(lastUpdate));
		} catch (Exception e) {
			log.error("Unable to save last update date: " + e.getMessage(), e);
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (tsRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("SQL Server Time Sheet records read", BatchJobService.FORMAT_INTEGER, new BigInteger(tsRecordsRead + "")));
		}
		if (tcRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("SQL Server Time Card records read", BatchJobService.FORMAT_INTEGER, new BigInteger(tcRecordsRead + "")));
		}
		if (usersFound > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow users found", BatchJobService.FORMAT_INTEGER, new BigInteger(usersFound + "")));
		}
		if (usersNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow users not found", BatchJobService.FORMAT_INTEGER, new BigInteger(usersNotFound + "")));
		}
		if (snTimeSheetsFound > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Time Sheets found", BatchJobService.FORMAT_INTEGER, new BigInteger(snTimeSheetsFound + "")));
		}
		if (snTimeSheetsNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Time Sheets not found", BatchJobService.FORMAT_INTEGER, new BigInteger(snTimeSheetsNotFound + "")));
		}
		if (snTimeCardsDeleted > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Time Card records deleted", BatchJobService.FORMAT_INTEGER, new BigInteger(snTimeCardsDeleted + "")));
		}
		if (snTimeSheetsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Time Sheet records inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(snTimeSheetsInserted + "")));
		}
		if (snTimeCardsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceNow Time Card records inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(snTimeCardsInserted + "")));
		}

		// end job
		log.info(" ");
		log.info("CopyTimeSheet complete.");

		return stats;
	}

	private void person(TimeSheet ts) throws Exception {
		String date = DF.format(ts.getDate());
		String userSysId = getUserSysId(ts.getUserId());
		if (StringUtils.isNotEmpty(userSysId)) {
			usersFound++;
			String tsSysId = getTimeSheetSysId(date, userSysId);
			if (StringUtils.isNotEmpty(tsSysId)) {
				PreparedStatement ps = conn.prepareStatement(TIMECARD_SQL);
				ps.setString(1, date);
				ps.setString(2, date);
				ps.setString(3, date);
				ps.setString(4, date);
				ps.setString(5, date);
				ps.setString(6, date);
				ps.setString(7, date);
				ps.setString(8, ts.getUserId());
				ps.setString(9, date);
				ps.setString(10, date);
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					tcRecordsRead++;
					insertTimeCard(userSysId, tsSysId, date, rs.getString("WORKITEM"), rs.getInt("SUNDAY"), rs.getInt("MONDAY"), rs.getInt("TUESDAY"), rs.getInt("WEDNESDAY"), rs.getInt("THURSDAY"), rs.getInt("FRIDAY"), rs.getInt("SATURDAY"));
				}
				rs.close();
				ps.close();
				lastUpdate = ts.getLastUpdate();
				if (log.isDebugEnabled()) {
					log.debug("Last update: " + DF2.format(lastUpdate));
				}
			} else {
				log.error("Unable to obtain a Time Sheet for user \"" + ts.getUserId() + "\"; processing terminated for this user.");
			}
		} else {
			usersNotFound++;
			log.error("User \"" + ts.getUserId() + "\" not found; processing terminated for this user.");
		}
	}

	private String getTimeSheetSysId(String date, String userSysId) {
		if (log.isDebugEnabled()) {
			log.debug("Searching for Time Sheet for date " + date + " for user " + userSysId);
		}

		String tsSysId = fetchTimeSheetSysId(date, userSysId);

		if (StringUtils.isNotEmpty(tsSysId)) {
			snTimeSheetsFound++;
			if (log.isDebugEnabled()) {
				log.debug("Time Sheet found for date " + date + " for user " + userSysId + ": " + tsSysId);
			}
			deleteOldTimeCards(tsSysId);
		} else {
			snTimeSheetsNotFound++;
			if (log.isDebugEnabled()) {
				log.debug("Time Sheet not found for date " + date + " for user " + userSysId + "; creating new Time Sheet.");
			}
			tsSysId = insertTimeSheet(date, userSysId);
		}

		return tsSysId;
	}

	private String fetchTimeSheetSysId(String date, String userSysId) {
		String tsSysId = null;

		String url = serviceNowServer + TIME_SHEET_SYSID_URL.replace("{date}", date) + userSysId;
		if (log.isDebugEnabled()) {
			log.debug("Fetching Time Sheet using URL " + url);
		}
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
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
						tsSysId = (String) obj.get("sys_id");
					}
				}
			} else {
				log.error("Invalid response received from HTTP GET: " + rc);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to fetch Time Sheet sys_id: " + e.toString(), e);
		}

		return tsSysId;
	}

	@SuppressWarnings("unchecked")
	private void deleteOldTimeCards(String tsSysId) {
		JSONArray timeCard = fetchTimeCards(tsSysId);
		if (timeCard != null && timeCard.size() > 0) {
			Iterator<JSONObject> i = timeCard.iterator();
			while (i.hasNext()) {
				JSONObject obj = i.next();
				String tcSysId = (String) obj.get("sys_id");
				if (StringUtils.isNotEmpty(tcSysId)) {
					deleteTimeCard(tcSysId);
				}
			}
		}
	}

	private JSONArray fetchTimeCards(String tsSysId) {
		JSONArray timeCard = null;

		String url = serviceNowServer + TIME_CARD_FETCH_URL + tsSysId;
		if (log.isDebugEnabled()) {
			log.debug("Fetching old Time Cards using URL " + url);
		}
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
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
					timeCard = (JSONArray) json.get("result");
				}
			} else {
				log.error("Invalid response received from HTTP GET: " + rc);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to fetch old Time Cards: " + e.toString(), e);
		}

		return timeCard;
	}

	private void deleteTimeCard(String tcSysId) {
		String url = serviceNowServer + TIME_CARD_URL + "/" + tcSysId;
		if (log.isDebugEnabled()) {
			log.debug("Deleting old Time Card using URL " + url);
		}
		try {
			HttpDelete delete = new HttpDelete(url);
			delete.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), delete, null));
			delete.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpResponse response = client.execute(delete);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200 || rc == 204) {
				if (log.isDebugEnabled()) {
					JSONObject json = (JSONObject) JSONValue.parse(resp);
					if (json != null) {
						log.debug("Delete response: " + json.toJSONString());
					} else {
						log.debug("HTTP response code from DELETE: " + rc);
					}
				}
			} else {
				log.error("Invalid response received from HTTP DELETE: " + rc);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to fetch old Time Cards: " + e.toString(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private String insertTimeSheet(String date, String userSysId) {
		String tsSysId = null;

		String url = serviceNowServer + TIME_SHEET_URL;
		// create HttpPost
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// put JSON
		try {
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			JSONObject timeSheet = new JSONObject();
			timeSheet.put("week_starts_on", date);
			timeSheet.put("user", userSysId);
			timeSheet.put("state", "Approved");
			timeSheet.put("comments", "Time Sheet created via Integration");
			post.setEntity(new StringEntity(timeSheet.toJSONString()));
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200 || rc == 201) {
				snTimeSheetsInserted++;
				JSONObject result = (JSONObject) JSONValue.parse(resp);
				if (result != null) {
					if (log.isDebugEnabled()) {
						log.debug("JSON response: " + result.toJSONString());
					}
					result = (JSONObject) result.get("result");
					if (result != null) {
						tsSysId = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty(tsSysId)) {
							if (log.isDebugEnabled()) {
								log.debug("New Time Sheet sys_id: " + tsSysId);
							}
						}
					}
				}
			} else {
				log.error("Invalid response from ServiceNow to POST: rc=" + rc + "; response=" + resp);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to insert Time Sheet: " + e.toString(), e);
		}

		return tsSysId;
	}

	@SuppressWarnings("unchecked")
	private void insertTimeCard(String userSysId, String tsSysId, String date, String category, int sunday, int monday, int tuesday, int wednesday, int thursday, int friday, int saturday) {
		String url = serviceNowServer + TIME_CARD_URL;
		// create HttpPost
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// put JSON
		try {
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			JSONObject timeCard = new JSONObject();
			timeCard.put("user", userSysId);
			timeCard.put("time_sheet", tsSysId);
			timeCard.put("week_starts_on", date);
			timeCard.put("category", category);
			timeCard.put("state", "Approved");
			timeCard.put("sunday", sunday + "");
			timeCard.put("monday", monday + "");
			timeCard.put("tuesday", tuesday + "");
			timeCard.put("wednesday", wednesday + "");
			timeCard.put("thursday", thursday + "");
			timeCard.put("friday", friday + "");
			timeCard.put("saturday", saturday + "");
			post.setEntity(new StringEntity(timeCard.toJSONString()));
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc == 200 || rc == 201) {
				snTimeCardsInserted++;
				JSONObject result = (JSONObject) JSONValue.parse(resp);
				if (result != null) {
					if (log.isDebugEnabled()) {
						log.debug("JSON response: " + result.toJSONString());
					}
					result = (JSONObject) result.get("result");
					if (result != null) {
						tsSysId = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty(tsSysId)) {
							if (log.isDebugEnabled()) {
								log.debug("Time Card sys_id: " + tsSysId);
							}
						}
					}
				}
			} else {
				log.error("Invalid response from ServiceNow to POST: rc=" + rc + "; response=" + resp);
			}
		} catch (Exception e) {
			log.error("Exception encounted when attempting to insert Time Card: " + e.toString(), e);
		}
	}

	private String getUserSysId(String userId) {
		String sysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching for user " + userId);
		}
		if (userSysId.containsKey(userId)) {
			sysId = userSysId.get(userId);
			if (log.isDebugEnabled()) {
				log.debug("User " + userId + " found in cache: " + sysId);
			}
		} else {
			sysId = fetchUserSysId(userId);
			if (StringUtils.isNotEmpty(sysId)) {
				 userSysId.put(userId, sysId);
				if (log.isDebugEnabled()) {
					log.debug("User " + userId + " fetched from ServiceNow: " + sysId);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User " + userId + " not found on ServiceNow");
				}
			}
		}

		return sysId;
	}

	private String fetchUserSysId(String iamId) {
		String sysId = "";

		String url = serviceNowServer + SYSID_URL + iamId;
		if (log.isDebugEnabled()) {
			log.debug("Fetching user sys_id using URL " + url);
		}
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
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

	private class TimeSheet {
		private String userId = null;
		private Date date = null;
		private Timestamp lastUpdate = null;

		public TimeSheet(String userId, Date date, Timestamp lastUpdate) {
			this.userId = userId;
			this.date = date;
			this.lastUpdate = lastUpdate;
		}

		public String getUserId() {
			return this.userId;
		}

		public Date getDate() {
			return this.date;
		}

		public Timestamp getLastUpdate() {
			return this.lastUpdate;
		}
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
