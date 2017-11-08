package edu.ucdavis.ucdh.itoc.snutil.batch;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.itoc.snutil.batch.util.SpringBatchJob;
import edu.ucdavis.ucdh.itoc.snutil.util.EventService;
import edu.ucdavis.ucdhs.isweb.core.utils.BatchJobService;
import edu.ucdavis.ucdhs.isweb.core.utils.BatchJobServiceStatistic;

/**
 * <p>Loads ServiceNow with user data from the HS Person Repository.</p>
 */
public class InitialUserLoad implements SpringBatchJob {
	private static final long DIFF_NET_JAVA_FOR_DATES = 11644473600000L + 24 * 60 * 60 * 1000;
	private static final String SEARCH_SQL = "SELECT ADDRESS, ALTERNATE_EMAIL, ALTERNATE_PHONES, BANNER_ID, BUILDING, CAMPUS_PPS_ID, CELL_NUMBER, CITY, CUBE, DEPT_ID, EMAIL, END_DATE, EXTERNAL_ID, FIRST_NAME, FLOOR, HS_AD_ID, ID, IS_ACTIVE, IS_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, IS_STUDENT, KERBEROS_ID, LAST_NAME, LOCATION_CODE, MANAGER, MIDDLE_NAME, MOTHRA_ID, PAGER_NUMBER, PAGER_PROVIDER, PHONE_NUMBER, PPS_ID, ROOM, START_DATE, STATE, STUDENT_ID, SUPERVISOR, TITLE, VOLUNTEER_ID, ZIP FROM VIEW_PERSON_ALL ORDER BY ID";
	private static final String IAM_SQL = "SELECT IAMID, PPSID, EXTERNALID, PRI_HSAD_ACCOUNT FROM PR_SERVICES.hs_sd_people where IAMID=?";
	private static final String FETCH_URL = "/api/now/table/sys_user?sysparm_display_value=all&sysparm_query=employee_number%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String BLDG_URL = "/api/now/table/cmn_building?sysparm_fields=sys_id&sysparm_query=name%3D";
	private static final String CC_URL = "/api/now/table/cmn_cost_center?sysparm_fields=sys_id&sysparm_query=account_number%3D";
	private static final String DEPT_URL = "/api/now/table/cmn_department?sysparm_fields=sys_id&sysparm_query=id%3D";
	private static final String LOC_URL = "/api/now/table/cmn_location?sysparm_fields=sys_id&sysparm_query=u_location_code%3D";
	private static final String UPDATE_URL = "/api/now/table/sys_user";
	private static final String PHOTO_URL = "/api/now/table/ecc_queue";
	private static final String LIVE_PROFILE_URL = "/api/now/table/live_profile";
	private static final String PHOTO_FETCH_URL = "/api/now/table/sys_user?sysparm_display_value=true&sysparm_fields=photo&sysparm_query=sys_id%3D";
	private static final String LIVE_PHOTO_FETCH_URL = "/api/now/table/live_profile?sysparm_display_value=true&sysparm_fields=photo&sysparm_query=sys_id%3D";
	private static final String ATTACHMENT_URL = "/api/now/table/sys_attachment/";
	private static final String PHOTO_EXISTS = "photo exists";
	private static final String[] PROPERTY = {"active:IS_ACTIVE", "building:BUILDING", "campus_pps_id:CAMPUS_PPS_ID", "city:CITY", "cost_center:DEPT_ID", "department:DEPT_ID", "email:EMAIL", "employee_number:ID", "first_name:FIRST_NAME", "last_name:LAST_NAME", "location:LOCATION_CODE", "manager:MANAGER", "middle_name:MIDDLE_NAME", "mobile_phone:CELL_NUMBER", "phone:PHONE_NUMBER", "state:STATE", "street:ADDRESS", "title:TITLE", "u_alternate_email:ALTERNATE_EMAIL", "u_alternate_phones:ALTERNATE_PHONES", "u_banner_id:BANNER_ID", "u_cube:CUBE", "u_end_date:END_DATE", "u_external_id:EXTERNAL_ID", "u_floor:FLOOR", "u_is_employee:IS_EMPLOYEE", "u_is_external:IS_EXTERNAL", "u_is_previous_ucdh_employee:IS_PREVIOUS_HS_EMPLOYEE", "u_is_student:IS_STUDENT", "u_kerberos_id:KERBEROS_ID", "u_mothra_id:MOTHRA_ID", "u_pager:PAGER_NUMBER", "u_pager_provider:PAGER_PROVIDER", "u_pps_id:PPS_ID", "u_room:ROOM", "u_start_date:START_DATE", "u_student_id:STUDENT_ID", "u_supervisor:SUPERVISOR", "u_volunteer_id:VOLUNTEER_ID", "user_name:HS_AD_ID", "zip:ZIP"};
	private static final String[] BOOLEAN_PROPERTY = {"active", "u_is_employee", "u_is_external", "u_is_previous_ucdh_employee", "u_is_student"};
	private static final String[] PERSON_PROPERTY = {"manager", "u_supervisor"};
	private final Log log = LogFactory.getLog(getClass().getName());
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,String> referenceURL = new HashMap<String,String>();
	private Map<String,Map<String,String>> referenceCache = new HashMap<String,Map<String,String>>();
	private Connection prConn = null;
	private Connection iamConn = null;
	private Connection psConn = null;
	private Connection ppsConn = null;
	private Connection badgeConn = null;
	private Connection portraitConn = null;
	private Statement prStmt = null;
	private ResultSet prRs = null;
	private Hashtable<String,String> env = null;
	private DirContext ctx = null;
	private List<String> notInIam = new ArrayList<String>();
	private List<String> multipleIamRecords = new ArrayList<String>();
	private List<String> noChanges = new ArrayList<String>();
	private List<String> inactive = new ArrayList<String>();
	private List<String> noUserName = new ArrayList<String>();
	private SearchControls ctls = null;
	private String[] searchOption = null;
	private String contextFactory = null;
	private String providerUrl = null;
	private String securityAuth = null;
	private String securityPrin = null;
	private String securityCred = null;
	private String standardSearch = null;
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;
	private DataSource prDataSource = null;
	private DataSource iamDataSource = null;
	private DataSource psDataSource = null;
	private DataSource ppsDataSource = null;
	private DataSource badgeDataSource = null;
	private DataSource portraitDataSource = null;
	private EventService eventService = null;
	private int recordsRead = 0;
	private int adAccountsFound = 0;
	private int recordsInserted = 0;
	private int recordsUpdated = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int jobId) throws Exception {
		loadBegin();
		while (prRs.next()) {
			person();
		}
		return loadEnd();
	}

	private void loadBegin() throws Exception {
		log.info("InitialUserLoad starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify contextFactory
		if (StringUtils.isEmpty(contextFactory)) {
			throw new IllegalArgumentException("Required property \"contextFactory\" missing or invalid.");
		} else {
			log.info("contextFactory = " + contextFactory);
		}
		// verify providerUrl
		if (StringUtils.isEmpty(providerUrl)) {
			throw new IllegalArgumentException("Required property \"providerUrl\" missing or invalid.");
		} else {
			log.info("providerUrl = " + providerUrl);
		}
		// verify securityAuth
		if (StringUtils.isEmpty(securityAuth)) {
			throw new IllegalArgumentException("Required property \"securityAuth\" missing or invalid.");
		} else {
			log.info("securityAuth = " + securityAuth);
		}
		// verify securityPrins
		if (StringUtils.isEmpty(securityPrin)) {
			throw new IllegalArgumentException("Required property \"securityPrin\" missing or invalid.");
		} else {
			log.info("securityPrin = " + securityPrin);
		}
		// verify securityCred
		if (StringUtils.isEmpty(securityCred)) {
			throw new IllegalArgumentException("Required property \"securityCred\" missing or invalid.");
		} else {
			log.info("securityCred = **********");
		}
		// verify Standard Search
		if (StringUtils.isEmpty(standardSearch)) {
			throw new IllegalArgumentException("Required property \"standardSearch\" missing or invalid.");
		} else {
			searchOption = standardSearch.split("\n");
			for (int i=0; i<searchOption.length; i++) {
				log.info("searchOption[" + i + "] = " + searchOption[i]);
			}
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
		// verify prDataSource
		if (prDataSource == null) {
			throw new IllegalArgumentException("Required property \"prDataSource\" missing or invalid.");
		} else {
			try {
				prConn = prDataSource.getConnection();
				log.info("Connection established to prDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to prDataSource: " + e, e);
			}
		}
		// verify iamDataSource
		if (iamDataSource == null) {
			throw new IllegalArgumentException("Required property \"iamDataSource\" missing or invalid.");
		} else {
			try {
				iamConn = iamDataSource.getConnection();
				log.info("Connection established to iamDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to iamDataSource: " + e, e);
			}
		}
		// verify psDataSource
		if (psDataSource == null) {
			throw new IllegalArgumentException("Required property \"psDataSource\" missing or invalid.");
		} else {
			try {
				psConn = psDataSource.getConnection();
				log.info("Connection established to psDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to psDataSource: " + e, e);
			}
		}
		// verify ppsDataSource
		if (ppsDataSource == null) {
			throw new IllegalArgumentException("Required property \"ppsDataSource\" missing or invalid.");
		} else {
			try {
				ppsConn = ppsDataSource.getConnection();
				log.info("Connection established to ppsDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to ppsDataSource: " + e, e);
			}
		}
		// verify badgeDataSource
		if (badgeDataSource == null) {
			throw new IllegalArgumentException("Required property \"badgeDataSource\" missing or invalid.");
		} else {
			try {
				badgeConn = badgeDataSource.getConnection();
				log.info("Connection established to badgeDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to badgeDataSource: " + e, e);
			}
		}
		// verify portraitDataSource
		if (portraitDataSource == null) {
			throw new IllegalArgumentException("Required property \"portraitDataSource\" missing or invalid.");
		} else {
			try {
				portraitConn = portraitDataSource.getConnection();
				log.info("Connection established to portraitDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to portraitDataSource: " + e, e);
			}
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// establish configuration variables
		for (int i=0;i<PROPERTY.length;i++) {
			String[] parts = PROPERTY[i].split(":");
			fieldMap.put(parts[0], parts[1]);
		}
		referenceCache.put("user", new HashMap<String,String>());
		referenceURL.put("building", BLDG_URL);
		referenceCache.put("building", new HashMap<String,String>());
		referenceURL.put("cost_center", CC_URL);
		referenceCache.put("cost_center", new HashMap<String,String>());
		referenceURL.put("department", DEPT_URL);
		referenceCache.put("department", new HashMap<String,String>());
		referenceURL.put("location", LOC_URL);
		referenceCache.put("location", new HashMap<String,String>());

		// establish LDAP search environment
		env = new Hashtable<String,String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
		env.put(Context.PROVIDER_URL, providerUrl);					   
		env.put(Context.SECURITY_AUTHENTICATION, securityAuth);					   
		env.put(Context.SECURITY_PRINCIPAL, securityPrin);	
		env.put(Context.SECURITY_CREDENTIALS, securityCred);
		ctx = new InitialDirContext(env);
		ctls = new SearchControls();
		ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

		// query person repository
		prStmt = prConn.createStatement();
		prRs = prStmt.executeQuery(SEARCH_SQL);
	}

	private List<BatchJobServiceStatistic> loadEnd() throws Exception {
		// close database connection
		prStmt.close();
		prRs.close();
		prConn.close();
		iamConn.close();
		psConn.close();
		ppsConn.close();
		badgeConn.close();
		portraitConn.close();

		// dump collected IAM IDs
		log.info(" ");
		log.info("notInIam: " + notInIam);
		log.info("multipleIamRecords: " + multipleIamRecords);
		log.info("noChanges: " + noChanges);
		log.info("inactive: " + inactive);
		log.info("noUserName: " + noUserName);
		log.info(" ");

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (recordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("External master records read", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsRead + "")));
		}
		if (adAccountsFound > 0) {
			stats.add(new BatchJobServiceStatistic("Active Directory records found", BatchJobService.FORMAT_INTEGER, new BigInteger(adAccountsFound + "")));
		}
		if (recordsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("External master records inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsInserted + "")));
		}
		if (recordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("External master records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsUpdated + "")));
		}
		if (notInIam.size() > 0) {
			stats.add(new BatchJobServiceStatistic("IAM IDs not in IAM", BatchJobService.FORMAT_INTEGER, new BigInteger(notInIam.size() + "")));
		}
		if (multipleIamRecords.size() > 0) {
			stats.add(new BatchJobServiceStatistic("IAM IDs in IAM multiple times", BatchJobService.FORMAT_INTEGER, new BigInteger(multipleIamRecords.size() + "")));
		}
		if (noChanges.size() > 0) {
			stats.add(new BatchJobServiceStatistic("Records with no changes", BatchJobService.FORMAT_INTEGER, new BigInteger(noChanges.size() + "")));
		}
		if (inactive.size() > 0) {
			stats.add(new BatchJobServiceStatistic("Inactive persons", BatchJobService.FORMAT_INTEGER, new BigInteger(inactive.size() + "")));
		}
		if (noUserName.size() > 0) {
			stats.add(new BatchJobServiceStatistic("Records with no username", BatchJobService.FORMAT_INTEGER, new BigInteger(noUserName.size() + "")));
		}

		// end job
		log.info(" ");
		log.info("InitialUserLoad complete.");

		return stats;
	}

	@SuppressWarnings("unchecked")
	private void person() throws Exception {
		recordsRead++;

		JSONObject details = new JSONObject();
		JSONObject newPerson = null;
		JSONObject oldPerson = null;
		String iamId = prRs.getString("ID");
		String sysId = null;
		if (log.isDebugEnabled()) {
			log.debug("Processing IAM ID: " + iamId);
		}
		JSONObject iamData = getIamData(iamId, details);
		if (iamData != null) {
			details.put("iamData", iamData);
			newPerson = getUcdhData(iamId);
			if (StringUtils.isEmpty((String) newPerson.get("user_name"))) {
				noUserName.add(iamId);
				JSONObject ldapData = getLdapData(iamId);
				if (ldapData != null) {
					details.put("ldapData", ldapData);
					if (StringUtils.isNotEmpty((String) ldapData.get("user_name"))) {
						newPerson.put("user_name",  ((String) ldapData.get("user_name")).toLowerCase());
						if (log.isDebugEnabled()) {
							log.debug("Using user_name from LDAP for IAM ID: " + iamId + "; user_name: " + newPerson.get("user_name"));
						}
					}
				}
			}
			details.put("newData", newPerson);
			oldPerson = fetchServiceNowUser(iamId, details);
			if (oldPerson != null) {
				if (StringUtils.isEmpty((String) newPerson.get("user_name"))) {
					String oldUserName = null;
					if (oldPerson.get("user_name") != null) {
						oldUserName = (String) ((JSONObject) oldPerson.get("user_name")).get("value");
					}
					if (StringUtils.isNotEmpty(oldUserName)) {
						newPerson.put("user_name", oldUserName);
						if (log.isDebugEnabled()) {
							log.debug("Retaining old username for " + iamId + ": " + newPerson.get("user_name"));
						}
					}
				}
				details.put("existingData", oldPerson);
				JSONObject sysIdInfo = (JSONObject) oldPerson.get("sys_id"); 
				if (sysIdInfo != null) {
					sysId = (String) sysIdInfo.get("value");
				}
				if (StringUtils.isNotEmpty(sysId)) {
					Map<String,String> cache = referenceCache.get("user");
					if (!cache.containsKey(iamId)) {
						cache.put(iamId, sysId);
					}
				}
				if (personUnchanged(newPerson, oldPerson)) {
					noChanges.add(iamId);
					if (log.isDebugEnabled()) {
						log.debug("Skipping IAM ID " + iamId + "; no changes detected.");
					}
				} else {
					updateServiceNowUser(newPerson, sysId);
				}
			} else {
				if ("false".equalsIgnoreCase((String) newPerson.get("active"))) {
					inactive.add(iamId);
					if (log.isDebugEnabled()) {
						log.debug("Skipping IAM ID " + iamId + "; new person is not active.");
					}
				} else {
					insertServiceNowUser(newPerson);
					sysId = (String) newPerson.get("sys_id");
					if (StringUtils.isNotEmpty(sysId)) {
						Map<String,String> cache = referenceCache.get("user");
						if (!cache.containsKey(iamId)) {
							cache.put(iamId, sysId);
						}
					}
				}
			}
		} else {
			notInIam.add(iamId);
			if (log.isDebugEnabled()) {
				log.debug("Skipping IAM ID " + iamId + "; not in IAM Person Repository.");
			}
		}

		// check photo
		if (StringUtils.isNotEmpty(sysId)) {
			boolean photoNeeded = true;
			boolean livePhotoNeeded = true;
			if (oldPerson != null) {
				JSONObject photoInfo = (JSONObject) oldPerson.get("photo");
				if (photoInfo != null) {
					if (StringUtils.isNotEmpty((String) photoInfo.get("display_value"))) {
						photoNeeded = false;
					}
				}
			}
			String liveProfileSysId = checkLiveProfile(sysId, details);
			if (PHOTO_EXISTS.equalsIgnoreCase(liveProfileSysId)) {
				livePhotoNeeded = false;
			}
			if (photoNeeded || livePhotoNeeded) {
				String base64PhotoData = fetchUserPhoto(newPerson, oldPerson, details);
				if (StringUtils.isNotEmpty(base64PhotoData)) {
					if (photoNeeded) {
						addPhoto(sysId, base64PhotoData, details);
					}
					if (livePhotoNeeded) {
						addLivePhoto(sysId, newPerson.get("first_name") + " " + newPerson.get("last_name"), liveProfileSysId, base64PhotoData, details);
					}
				}
			}
		}
	}

	/**
	 * <p>Returns the IAM data associated with passed iamId.</p>
	 *
	 * @param iamId the iamId for this person
	 * @param details the details related to this transaction
	 * @return the IAM data associated with passed iamId
	 */
	@SuppressWarnings("unchecked")
	private JSONObject getIamData(String iamId, JSONObject details) {
		JSONObject iamData = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching IAM data for " + iamId);
		}
		List<String> iamIds = new ArrayList<String>();
		List<String> ppsIds = new ArrayList<String>();
		List<String> extIds = new ArrayList<String>();
		List<String> adIds = new ArrayList<String>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = iamDataSource.getConnection();
			ps = conn.prepareStatement(IAM_SQL);
			ps.setString(1, iamId);
			rs = ps.executeQuery();
			while (rs.next()) {
				iamIds.add(rs.getString("IAMID") + "");
				ppsIds.add(rs.getString("PPSID") + "");
				extIds.add(rs.getString("EXTERNALID") + "");
				adIds.add(rs.getString("PRI_HSAD_ACCOUNT") + "");
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing IAM data for "  + iamId + ": " + e.getMessage(), e);
			//eventService.logEvent(new Event("Java Batch Error", "IAM data fetch exception", null, "Exception encountered accessing IAM data for "  + iamId + ": " + e.getMessage(), null, details));
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
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}
		if (iamIds.size() > 0) {
			if (iamIds.size() > 1) {
				multipleIamRecords.add(iamId);
				if (log.isDebugEnabled()) {
					log.debug(iamIds.size() + " IAM data records found for " + iamId);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("IAM data found for " + iamId);
				}
			}
			iamData = new JSONObject();
			Iterator<String> i = ppsIds.iterator();
			while (i.hasNext()) {
				String ppsId = i.next();
				if (StringUtils.isNotEmpty(ppsId)) {
					iamData.put("PPS ID", ppsId);
				}
			}
			i = extIds.iterator();
			while (i.hasNext()) {
				String extId = i.next();
				if (StringUtils.isNotEmpty(extId)) {
					if (extId.startsWith("H")) {
						iamData.put("EXTERNAL ID", extId);
					}
				}
			}
			i = adIds.iterator();
			while (i.hasNext()) {
				String adId = i.next();
				if (StringUtils.isNotEmpty(adId)) {
					iamData.put("EXTERNAL ID", adId);
				}
			}
		}

		return iamData;
	}

	@SuppressWarnings("unchecked")
	private JSONObject getUcdhData(String iamId) throws Exception {
		JSONObject person = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			person.put(target, prRs.getString(source));
		}

		for (int x=0; x<BOOLEAN_PROPERTY.length; x++) {
			String field = BOOLEAN_PROPERTY[x];
			if ("Y".equalsIgnoreCase((String) person.get(field))) {
				person.put(field, "true");
			} else {
				person.put(field, "false");
			}
		}

		for (int x=0; x<PERSON_PROPERTY.length; x++) {
			String field = PERSON_PROPERTY[x];
			if (StringUtils.isNotEmpty((String) person.get(field))) {
				person.put(field, getUserSysId((String) person.get(field)));
			}
		}

		i = referenceURL.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) person.get(field))) {
				person.put(field, getReferenceSysId(field, (String) person.get(field)));
			}
		}
		
		if (StringUtils.isNotEmpty((String) person.get("u_start_date")) && ((String) person.get("u_start_date")).length() > 10) {
			person.put("u_start_date", ((String) person.get("u_start_date")).substring(0, 10));
		}
		if (StringUtils.isNotEmpty((String) person.get("u_end_date")) && ((String) person.get("u_end_date")).length() > 10) {
			person.put("u_end_date", ((String) person.get("u_end_date")).substring(0, 10));
		}

		if (log.isDebugEnabled()) {
			log.debug("Returning new user values: " + person);
		}

		return person;
	}

	@SuppressWarnings("unchecked")
	private JSONObject getLdapData(String iamId) throws Exception {
		JSONObject ldapData = null;

		for (int i=0; i<searchOption.length; i++) {
			NamingEnumeration<SearchResult> e = ctx.search(searchOption[i], "employeeNumber=" + iamId, ctls);
			if (e.hasMoreElements()) {
				SearchResult sr = e.nextElement();
				adAccountsFound++;
				if (log.isDebugEnabled()) {
					log.debug("A/D record found for " + iamId);
				}
				ldapData = new JSONObject();
				ldapData.put("employee_number", iamId);
				if (sr.getAttributes().get("sAMAccountName") != null) {
					if (log.isDebugEnabled()) {
						log.debug("sAMAccountName attribute present for " + iamId);
					}
					ldapData.put("user_name", sr.getAttributes().get("sAMAccountName").get());
				}
				if (sr.getAttributes().get("employeeID") != null) {
					if (log.isDebugEnabled()) {
						log.debug("employeeID attribute present for " + iamId);
					}
					ldapData.put("pps_id", sr.getAttributes().get("employeeID").get());
				}
				if (sr.getAttributes().get("pwdLastSet") != null) {
					if (log.isDebugEnabled()) {
						log.debug("pwdLastSet attribute present for " + iamId);
					}
					ldapData.put("account_claimed", getTimeStamp(sr.getAttributes().get("pwdLastSet")));
				}
				if (sr.getAttributes().get("lastLogonTimestamp") != null) {
					if (log.isDebugEnabled()) {
						log.debug("lastLogonTimestamp attribute present for " + iamId);
					}
					ldapData.put("account_used", getTimeStamp(sr.getAttributes().get("lastLogonTimestamp")));
				}
				i = searchOption.length;
			}
		}
		if (log.isDebugEnabled()) {
			if (ldapData != null) {
				log.debug("Returning ldapData for IAM ID " + iamId + ": " + ldapData);
			} else {
				log.debug("No ldapData found for IAM ID " + iamId);
			}
		}

		return ldapData;
	}

	/**
	 * <p>Returns the ServiceNow user data on file, if present.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @return the ServiceNow user data
	 */
	private JSONObject fetchServiceNowUser(String iamId, JSONObject details) {
		JSONObject user = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow user data for IAM ID " + iamId);
		}
		String url = "";
		try {
			url = serviceNowServer + FETCH_URL + URLEncoder.encode(iamId, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			url = serviceNowServer + FETCH_URL + iamId;
		}
		HttpGet get = new HttpGet(url);
		get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user data for IAM ID " + iamId + ": " + rc);
					//eventService.logEvent(new Event("ServletError", "User fetch error", null, "Invalid HTTP Response Code returned when fetching user data for IAM ID " + iamId + ": " + rc, null, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray users = (JSONArray) result.get("result");
			if (users != null && users.size() > 0) {
				user = (JSONObject) users.get(0);
				if (log.isDebugEnabled()) {
					log.debug("User found for IAM ID " + iamId + ": " + user);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User not found for IAM ID " + iamId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user with IAM ID " + iamId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "User fetch exception", null, "Exception encountered searching for user with IAM ID " + iamId + ": " + e, null, details));
		}

		return user;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param oldPerson the existing data for this person
	 * @return true if the person is unchanged
	 */
	private boolean personUnchanged(JSONObject newPerson, JSONObject oldPerson) {
		boolean unchanged = true;

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!isEqual(newPerson, oldPerson, field)) {
				unchanged = false;
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPerson the new data for this person
	 * @return the response
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	private void insertServiceNowUser(JSONObject newPerson) {
		if (log.isDebugEnabled()) {
			log.debug("Inserting person " + newPerson.get("employee_number"));
		}		
		
		// create HttpPost
		String url = serviceNowServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("company", "613323760f113200277e06bce1050ebb");
		insertData.put("date_format", "MM-dd-yyyy");
		insertData.put("preferred_language", "en");
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) newPerson.get(field))) {
				insertData.put(field, newPerson.get(field));
			}
		}
		if (StringUtils.isEmpty((String) insertData.get("user_name"))) {
			insertData.put("user_name", insertData.get("u_external_id"));
		}
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + insertData.toJSONString());
		}

		// post parameters
		try {
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting JSON data to " + url);
			}
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200 || rc == 201) {
				recordsInserted++;
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new user: " + rc);
				}
			}
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
					JSONObject result = (JSONObject) jsonResponse.get("result");
					if (result != null) {
						String sysId = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty("sys_id")) {
							newPerson.put("sys_id", sysId);
							if (log.isDebugEnabled()) {
								log.debug("sys_id for inserted person: " + sysId);
							}
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + jsonRespString);
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new user " + newPerson.get("employee_number") + ": " + e);
		}
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param sysId the existing person's ServiceNow sys_id
	 * @return the response
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	private void updateServiceNowUser(JSONObject newPerson, String sysId) {
		if (log.isDebugEnabled()) {
			log.debug("Updating person " + newPerson.get("employee_number"));
		}		
		
		// create HttpPut
		String url = serviceNowServer + UPDATE_URL + "/" + sysId;
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if ("true".equalsIgnoreCase((String) newPerson.get(field + "HasChanged"))) {
				String value = (String) newPerson.get(field);
				if (StringUtils.isNotEmpty(value)) {
					updateData.put(field, value);
				} else {
					updateData.put(field, "");
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("JSON object to PUT: " + updateData.toJSONString());
		}

		// put JSON
		try {
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON update to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200) {
				recordsUpdated++;
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc);
				}
			}
			if (log.isDebugEnabled()) {
				String jsonRespString = "";
				HttpEntity entity = resp.getEntity();
				if (entity != null) {
					jsonRespString = EntityUtils.toString(entity);
				}
				JSONObject result = (JSONObject) JSONValue.parse(jsonRespString);
				log.debug("JSON response: " + result.toJSONString());
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to update user " + newPerson.get("employee_number") + ": " + e);
		}
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the person for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the iamId passed
	 * @throws UnsupportedEncodingException 
	 */
	private String getUserSysId(String iamId) {
		String sysId = null;

		Map<String,String> cache = referenceCache.get("user");
		if (cache.containsKey(iamId)) {
			sysId = cache.get(iamId);
		} else {
			sysId = fetchUserSysId(iamId);
			if (StringUtils.isNotEmpty(sysId)) {
				cache.put(iamId, sysId);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning sys_id " + sysId + " for IAM ID " + iamId);
		}

		return sysId;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the person for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the iamId passed
	 */
	private String getReferenceSysId(String field, String value) {
		String returnValue = null;

		Map<String,String> cache = referenceCache.get(field);
		if (cache.containsKey(value)) {
			returnValue = cache.get(value);
		} else {
			returnValue = fetchReferenceSysId(field, value);
			if (StringUtils.isNotEmpty(returnValue)) {
				cache.put(value, returnValue);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning sys_id " + returnValue + " for " + field + " " + value);
		}

		return returnValue;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the person for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the iamId passed
	 * @throws UnsupportedEncodingException 
	 */
	private String fetchUserSysId(String iamId) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for IAM ID " + iamId);
		}

		String url = "";
		try {
			url = serviceNowServer + SYSID_URL + URLEncoder.encode(iamId, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			url = serviceNowServer + SYSID_URL + iamId;
		}
		HttpGet get = new HttpGet(url);
		get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for IAM ID " + iamId + ": " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					JSONObject obj = (JSONObject) result.get(0);
					sysId = (String) obj.get("sys_id");
					if (StringUtils.isNotEmpty(sysId)) {
						if (log.isDebugEnabled()) {
							log.debug("sys_id found for IAM ID " + iamId + ": " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for IAM ID " + iamId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for IAM ID " + iamId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "User sys_id fetch exception", "Exception encountered searching for sys_id for IAM ID " + iamId + ": " + e));
		}

		return sysId;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the information passed.</p>
	 *
	 * @param field the field for which the sys_id is being requested
	 * @param value the field value for which the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the information passed
	 */
	private String fetchReferenceSysId(String field, String value) {
		String sysId = "";

		try {
			String url = serviceNowServer + referenceURL.get(field) + URLEncoder.encode(value, "UTF-8");
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id for " + field + " " + value + " from URL " + url);
			}
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for " + field + " " + value + ": " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					JSONObject obj = (JSONObject) result.get(0);
					sysId = (String) obj.get("sys_id");
					if (StringUtils.isNotEmpty(sysId)) {
						if (log.isDebugEnabled()) {
							log.debug("sys_id found for " + field + " " + value + ": " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for " + field + " " + value);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for " + field + " " + value + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", field + " fetch exception", "Exception encountered searching for sys_id for " + field + " " + value + ": " + e));
		}

		return sysId;
	}

	/**
	 * <p>Returns the base64 encoded value of the image data.</p>
	 *
	 * @param newPerson the new person
	 * @param oldPerson the old person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the base64 encoded value of the image data
	 */
	private String fetchUserPhoto(Map<String,String> newPerson, JSONObject oldPerson, JSONObject details) {
		String base64PhotoData = null;

		String photoId = getPhotoId(newPerson, details);
		if (StringUtils.isNotEmpty(photoId)) {
			byte[] imageData = fetchImageFileData(photoId, details);
			if (imageData != null) {
				base64PhotoData = new String(Base64.getMimeEncoder().encode(imageData));
			}
		}

		return base64PhotoData;
	}

	/**
	 * <p>Returns the photo id associated with passed person.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the photo id associated with this person
	 */
	private String getPhotoId(Map<String,String> newPerson, JSONObject details) {
		String photoId = null;

		String iamId = newPerson.get("employee_number");
		String ppsId = newPerson.get("u_pps_id");
		if (log.isDebugEnabled()) {
			log.debug("Fetching photoId from badge system for " + iamId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = badgeDataSource.getConnection();
			ps = conn.prepareStatement("select ut_cardholder_id from udftext where ut_udfgen_id=28 AND ut_text=?");
			ps.setString(1, iamId);
			rs = ps.executeQuery();
			if (rs.next()) {
				photoId = rs.getString(1);
				if (log.isDebugEnabled()) {
					log.debug("photoId from iamId: " + photoId);
				}
			}
			if (StringUtils.isEmpty(photoId) && StringUtils.isNotEmpty(ppsId)) {
				rs.close();
				ps.close();
				if (log.isDebugEnabled()) {
					log.debug("photoId not found using iamId: " + iamId + "; trying again using ppsId: " + ppsId);
				}
				ps = conn.prepareStatement("select c_id from cardholder where c_nick_name=?");
				ps.setString(1, ppsId);
				rs = ps.executeQuery();
				if (rs.next()) {
					photoId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("photoId from ppsId: " + photoId);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing photoId: " + e.getMessage(), e);
			//eventService.logEvent(new Event("ServletError", "Photo ID fetch exception", "Exception encountered accessing photoId: " + e.getMessage(), details, e));
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
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Photo ID from badge system for " + iamId + ": " + photoId);
		}

		return photoId;
	}

	/**
	 * <p>Returns the byte array of the image data.</p>
	 *
	 * @param id the id of the photo data record
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the byte array of the image data
	 */
	private byte[] fetchImageFileData(String id, JSONObject details) {
		byte[] imageData = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching image data ...");
		}
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = portraitDataSource.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select pt_image from portrait where pt_cardholder_id=" + id);
			if (rs.next()) {
				imageData = rs.getBytes(1);
				if (log.isDebugEnabled()) {
					log.debug("image size: " + imageData.length);
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing image data: " + e.getMessage(), e);
			//eventService.logEvent(new Event("ServletError", "Photo processing exception", "Exception encountered processing image data: " + e.getMessage(), details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
					//
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}

		return imageData;
	}

	/**
	 * <p>Returns the sys_id of the ServiceNow Live Profile, if present.</p>
	 *
	 * @param sysId the sys_id of the user
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the sys_id of the ServiceNow Live Profile, if present
	 */
	private String checkLiveProfile(String sysId, JSONObject details) {
		String liveProfileSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow Live Profile for User sys_id " + sysId);
		}
		String url = serviceNowServer + LIVE_PROFILE_URL + "?sysparm_display_value=all&sysparm_fields=sys_id%2Cphoto&sysparm_query=document%3D" + sysId;
		HttpGet get = new HttpGet(url);
		get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching Live Profile data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching Live Profile data for sys_id " + sysId + ": " + rc);
					//eventService.logEvent(new Event("ServletError", "Live Profile fetch error", "Invalid HTTP Response Code returned when fetching Live Profile data for sys_id " + sysId + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray profiles = (JSONArray) result.get("result");
			if (profiles != null && profiles.size() > 0) {
				JSONObject profile = (JSONObject) profiles.get(0);
				if (log.isDebugEnabled()) {
					log.debug("Live Profile found for sys_id " + sysId + ": " + profile);
				}
				JSONObject photoData = (JSONObject) profile.get("photo");
				if (photoData != null) {
					if (StringUtils.isNotEmpty((String) photoData.get("display_value")) || StringUtils.isNotEmpty((String) photoData.get("value"))) {
						liveProfileSysId = PHOTO_EXISTS;
						if (log.isDebugEnabled()) {
							log.debug("Live Profile photo found for sys_id " + sysId + ": " + profile);
						}
					}
				}
				if (StringUtils.isEmpty(liveProfileSysId)) {
					JSONObject documentData = (JSONObject) profile.get("document");
					if (documentData != null) {
						liveProfileSysId = (String) documentData.get("value");
					}
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Live Profile not found for sys_id " + sysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for Live Profile with sys_id " + sysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "Live Profile fetch exception", "Exception encountered searching for Live Profile with sys_id " + sysId + ": " + e, details, e));
		}

		return liveProfileSysId;
	}

	/**
	 *  <p>Adds the user's photo</p>
	 *  
	 * @param sysId the sys_id of the user
	 * @param base64PhotoData the user's photo in base64 encoded format
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the attempt to add the photo
	 */
	private String addPhoto(String sysId, String base64PhotoData, JSONObject details) {
		String outcome = "";

		if (log.isDebugEnabled()) {
			log.debug("Adding photo to sys_id " + sysId);
		}
		// create HttpPost
		String url = serviceNowServer + PHOTO_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "text/xml");

		// build XML to post
		String xml="<request>\n";
		xml += "<entry>\n";
		xml += "<agent>UserUpdateServlet</agent>\n";
		xml += "<topic>AttachmentCreator</topic>\n";
		xml += "<name>photo:image/jpeg</name>\n";
		xml += "<source>sys_user:" + sysId + "</source>\n";
		xml += "<payload>" + base64PhotoData + "</payload>\n";
		xml += "</entry>\n";
		xml += "</request>";

		// post XML
		try {
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting XML photo request to " + url);
			}
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			if (rc == 200 || rc == 201) {
				outcome = "; photo added to user profile";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				outcome = "; unable to add user photo due to error";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when posting photo for sys_id " + sysId + ": " + rc);
				}
				//eventService.logEvent(new Event("ServletError", "Photo posting error", "Invalid HTTP Response Code returned when posting photo for sys_id " + sysId + ": " + rc, details));
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONObject result = (JSONObject) json.get("result");
				if (result != null) {
					String error = (String) result.get("error_string");
					if (StringUtils.isNotEmpty(error)) {
						outcome = "; unable to add user photo due to error";
						log.error("Error attempting to add user photo: " + error);
						//eventService.logEvent(new Event("ServletError", "Photo posting error", "Error attempting to add user photo: " + error, details));
					} else {
						if (log.isDebugEnabled()) {
							log.debug(result.get("payload"));
						}
						String userPhotoSysId = fetchUserPhotoSysId(sysId, details);
						updateAttachment(userPhotoSysId, "sys_user", details);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occured when posting photo for sys_id " + sysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "Photo posting exception", "Exception occured when posting photo for sys_id " + sysId + ": " + e, details, e));
			outcome = "; unable to add user photo due to exception";
		}

		return outcome;
	}

	/**
	 *  <p>Adds the user's Live Profile photo</p>
	 *  
	 * @param sysId the sys_id of the user
	 * @param name the name of the user
	 * @param liveProfileSysId the sys_id of the user's ServiceNow Live Profile
	 * @param base64PhotoData the user's photo in base64 encoded format
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the attempt to add the photo
	 */
	private String addLivePhoto(String sysId, String name, String liveProfileSysId, String base64PhotoData, JSONObject details) {
		String outcome = "";

		if (log.isDebugEnabled()) {
			log.debug("Adding Live Profile photo to sys_id " + sysId);
		}
		if (StringUtils.isEmpty(liveProfileSysId)) {
			liveProfileSysId = insertLiveProfile(sysId, name, details);
		}
		String livePhotoSysId = null;
		// create HttpPost
		String url = serviceNowServer + PHOTO_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "text/xml");

		// build XML to post
		String xml="<request>\n";
		xml += "<entry>\n";
		xml += "<agent>UserUpdateServlet</agent>\n";
		xml += "<topic>AttachmentCreator</topic>\n";
		xml += "<name>photo:image/jpeg</name>\n";
		xml += "<source>live_profile:" + liveProfileSysId + "</source>\n";
		xml += "<payload>" + base64PhotoData + "</payload>\n";
		xml += "</entry>\n";
		xml += "</request>";

		// post XML
		try {
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting XML Live Profile photo request to " + url);
			}
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			if (rc == 200 || rc == 201) {
				outcome = "; photo added to user's Live Profile";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				outcome = "; unable to add user's Live Profile photo due to error";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when posting Live Profile photo for sys_id " + sysId + ": " + rc);
				}
				//eventService.logEvent(new Event("ServletError", "Photo posting error", "Invalid HTTP Response Code returned when posting Live Profile photo for sys_id " + sysId + ": " + rc, details));
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONObject result = (JSONObject) json.get("result");
				if (result != null) {
					String error = (String) result.get("error_string");
					if (StringUtils.isNotEmpty(error)) {
						outcome = "; unable to add user's Live Profile photo due to error";
						log.error("Error attempting to add user's Live Profile photo: " + error);
						//eventService.logEvent(new Event("ServletError", "Photo posting error", "Error attempting to add user's Live Profile photo: " + error, details));
					} else {
						livePhotoSysId = fetchLiveProfilePhotoSysId(liveProfileSysId, details);
						if (log.isDebugEnabled()) {
							log.debug("Live Profile photo sys_id: " + livePhotoSysId);
						}
						updateAttachment(livePhotoSysId, "live_profile", details);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occured when posting Live Profile photo for sys_id " + sysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "Photo posting exception", "Exception occured when posting Live Profile photo for sys_id " + sysId + ": " + e, details, e));
			outcome = "; unable to add user's Live Profile photo due to exception";
		}
		if (StringUtils.isNotEmpty(livePhotoSysId)) {
			outcome += addLiveThumbnail(livePhotoSysId, base64PhotoData, details);
		}

		return outcome;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param attachmentSysId the sys_id of the attachment
	 * @param table the name of the table associated with the attachment
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 */
	@SuppressWarnings("unchecked")
	private void updateAttachment(String attachmentSysId, String table, JSONObject details) {
		if (log.isDebugEnabled()) {
			log.debug("Updating attachment " + attachmentSysId + " for table " + table);
		}		
		
		// create HttpPut
		String url = serviceNowServer + ATTACHMENT_URL + "/" + attachmentSysId;
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		updateData.put("table_name", "ZZ_YY" + table);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to PUT: " + updateData.toJSONString());
		}

		// put JSON
		try {
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON update to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200) {
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating attachment " + attachmentSysId + " for table " + table + ": " + rc);
				}
				//eventService.logEvent(new Event("ServletError", "Attachment update error", "Invalid HTTP Response Code returned when updating attachment " + attachmentSysId + " for table " + table + ": " + rc, details));
			}
			if (log.isDebugEnabled()) {
				String jsonRespString = "";
				HttpEntity entity = resp.getEntity();
				if (entity != null) {
					jsonRespString = EntityUtils.toString(entity);
				}
				JSONObject result = (JSONObject) JSONValue.parse(jsonRespString);
				log.debug("JSON response: " + result.toJSONString());
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to update attachment " + attachmentSysId + " for table " + table + ": " + e);
			//eventService.logEvent(new Event("ServletError", "Attachment update exception", "Exception occured when attempting to update attachment " + attachmentSysId + " for table " + table + ": " + e, details, e));
		}
	}

	/**
	 * <p>Returns the sys_id of the user photo, if present.</p>
	 *
	 * @param sysId the sys_id of the user
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return  the sys_id of the user photo, if present
	 */
	private String fetchUserPhotoSysId(String sysId, JSONObject details) {
		String userPhotoSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching user photo sys_id for user Profile " + sysId);
		}
		String url = serviceNowServer + PHOTO_FETCH_URL + sysId;
		HttpGet get = new HttpGet(url);
		get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user photo sys_id using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user photo sys_id for user " + sysId + ": " + rc);
					//eventService.logEvent(new Event("ServletError", "User fetch error", "Invalid HTTP Response Code returned when fetching user photo sys_id for user " + sysId + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray profiles = (JSONArray) result.get("result");
			if (profiles != null && profiles.size() > 0) {
				JSONObject profile = (JSONObject) profiles.get(0);
				userPhotoSysId = (String) profile.get("photo");
				if (StringUtils.isNotEmpty(userPhotoSysId)) {
					if (userPhotoSysId.indexOf(".") != -1) {
						userPhotoSysId = userPhotoSysId.substring(0, userPhotoSysId.indexOf("."));
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isNotEmpty(userPhotoSysId)) {
					log.debug("user photo sys_id for user " + sysId + ": " + userPhotoSysId);
				} else {
					log.debug("user photo sys_id not found for user " + sysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user photo sys_id for user " + sysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "User fetch exception", "Exception encountered searching for user photo sys_id for user " + sysId + ": " + e, details, e));
		}

		return userPhotoSysId;
	}

	/**
	 * <p>Returns the sys_id of the Live Profile Photo, if present.</p>
	 *
	 * @param liveProfileSysId the IAM ID of the person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return  the sys_id of the Live Profile Photo, if present
	 */
	private String fetchLiveProfilePhotoSysId(String liveProfileSysId, JSONObject details) {
		String livePhotoSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId);
		}
		String url = serviceNowServer + LIVE_PHOTO_FETCH_URL + liveProfileSysId;
		HttpGet get = new HttpGet(url);
		get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching Live Profile Photo sys_id using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + rc);
					//eventService.logEvent(new Event("ServletError", "User fetch error", "Invalid HTTP Response Code returned when fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray profiles = (JSONArray) result.get("result");
			if (profiles != null && profiles.size() > 0) {
				JSONObject profile = (JSONObject) profiles.get(0);
				livePhotoSysId = (String) profile.get("photo");
				if (StringUtils.isNotEmpty(livePhotoSysId)) {
					if (livePhotoSysId.indexOf(".") != -1) {
						livePhotoSysId = livePhotoSysId.substring(0, livePhotoSysId.indexOf("."));
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isNotEmpty(livePhotoSysId)) {
					log.debug("Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + livePhotoSysId);
				} else {
					log.debug("Live Profile Photo sys_id not found for Live Profile " + liveProfileSysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "User fetch exception", "Exception encountered searching for Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + e, details, e));
		}

		return livePhotoSysId;
	}

	/**
	 *  <p>Adds the user's Live Profile photo thumbnail</p>
	 *  
	 * @param liveProfileSysId the sys_id of the user's ServiceNow Live Profile
	 * @param base64PhotoData the user's photo in base64 encoded format
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the attempt to add the photo
	 */
	private String addLiveThumbnail(String livePhotoSysId, String base64PhotoData, JSONObject details) {
		String outcome = "";

		if (log.isDebugEnabled()) {
			log.debug("Adding thumbnail of Live Profile photo " + livePhotoSysId);
		}

		// create thumbnail
		base64PhotoData = createThumbnail(base64PhotoData, details);

		// create HttpPost
		String url = serviceNowServer + PHOTO_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "text/xml");

		// build XML to post
		String xml="<request>\n";
		xml += "<entry>\n";
		xml += "<agent>UserUpdateServlet</agent>\n";
		xml += "<topic>AttachmentCreator</topic>\n";
		xml += "<name>thumb_" + livePhotoSysId + "_150:image/jpeg</name>\n";
		xml += "<source>sys_attachment:" + livePhotoSysId + "</source>\n";
		xml += "<payload>" + base64PhotoData + "</payload>\n";
		xml += "</entry>\n";
		xml += "</request>";

		// post XML
		try {
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting XML Live Profile thumbnail request to " + url);
			}
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			if (rc == 200 || rc == 201) {
				outcome = "; thumbnail added to user's Live Profile";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				outcome = "; unable to add user's Live Profile thumbnail due to error";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when posting thumbnail for Live Profile photo " + livePhotoSysId + ": " + rc);
				}
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONObject result = (JSONObject) json.get("result");
				if (result != null) {
					String error = (String) result.get("error_string");
					if (StringUtils.isNotEmpty(error)) {
						log.error("Error attempting to add user's Live Profile thumbnail: " + error);
						//eventService.logEvent(new Event("ServletError", "Thumbnail posting error", "Error attempting to add user's Live Profile thumbnail: " + error, details));
					} else {
						if (log.isDebugEnabled()) {
							log.debug(result.get("payload"));
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occured when posting thumbnail for Live Profile photo " + livePhotoSysId + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "Thumbnail posting exception", "Exception occured when posting thumbnail for Live Profile photo " + livePhotoSysId + ": " + e, details, e));
			outcome = "; unable to add user's Live Profile thumbnail due to exception";
		}

		return outcome;
	}
	 
	/**
	 * <p>Creates an 152px wide thumbnail version of the passed image.</p>
	 * 
	 * @param base64PhotoData the user's photo in base64 encoded format
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return a thumbnail image of the user's photo in base64 encoded format
	 */
	public String createThumbnail(String base64PhotoData, JSONObject details) {
		String base64ThumbData = null;

		try {
			byte[] imageData = Base64.getMimeDecoder().decode(base64PhotoData);
			ByteArrayInputStream is = new ByteArrayInputStream(imageData);
			BufferedImage inputImage = ImageIO.read(is);
			int width = inputImage.getWidth();
			double ratio = (double) 152 / (double) width;
			int thumbWidth = 152;
			int thumbHeight = (int) ((double) inputImage.getHeight() * ratio);
			BufferedImage outputImage = new BufferedImage(thumbWidth, thumbHeight, inputImage.getType());
			Graphics2D g2d = outputImage.createGraphics();
			g2d.drawImage(inputImage, 0, 0, thumbWidth, thumbHeight, null);
			g2d.dispose();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			if (outputImage.getHeight() > 152) {
				int offset = (outputImage.getHeight() - 152) / 3;
				outputImage = outputImage.getSubimage(0, offset, 152, 152);
			}
			ImageIO.write(outputImage, "jpeg", os);
			byte[] thumbData = os.toByteArray();
			base64ThumbData = new String(Base64.getMimeEncoder().encode(thumbData));
		} catch (Exception e) {
			log.error("Exception occured when creating thumbnail image fron user photo: " + e, e);
			//eventService.logEvent(new Event("ServletError", "Thumbnail posting exception", "Exception occured when creating thumbnail image fron user photo: " + e, details, e));
		}

		return base64ThumbData;
	}

	/**
	 * <p>Inserts a ServiceNow Live Profile for the user and returns the sys_id of the profile.</p>
	 *
	 * @param sysId the user's sys_id
	 * @param name the user's name
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the sys_id of the Live Profile
	 */
	@SuppressWarnings("unchecked")
	private String insertLiveProfile(String sysId, String name, JSONObject details) {
		String liveProfileSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting Live Profile " + name);
		}		
		
		// create HttpPost
		String url = serviceNowServer + LIVE_PROFILE_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("table", "sys_user");
		insertData.put("type", "user");
		insertData.put("document", sysId);
		insertData.put("name", name);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + insertData.toJSONString());
		}

		// post parameters
		try {
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting JSON data to " + url);
			}
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200 || rc == 201) {
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting Live Profile: " + rc);
				}
			}
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
					if (log.isDebugEnabled()) {
						log.debug("JSON response from Live Profile insert: " + jsonRespString);
					}
					JSONObject result = (JSONObject) jsonResponse.get("result");
					if (result != null) {
						liveProfileSysId = (String) result.get("sys_id");
						if (log.isDebugEnabled()) {
							log.debug("sys_id for inserted Live Profile: " + sysId);
						}
					}
				}
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert Live Profile for " + name + ": " + e, e);
			//eventService.logEvent(new Event("ServletError", "User insert exception", "Exception occured when attempting to insert Live Profile for " + name + ": " + e, details, e));
		}

		return liveProfileSysId;
	}

	@SuppressWarnings("unchecked")
	private boolean isEqual(JSONObject p1, JSONObject p2, String key) {
		boolean eq = true;

		String s1 = (String) p1.get(key);
		String s2 = null;
		JSONObject fieldInfo = (JSONObject) p2.get(key);
		if (fieldInfo != null) {
			s2 = (String) fieldInfo.get("value");
		}
		if (key.endsWith("_date")) {
			if (StringUtils.isNotEmpty(s1) && s1.length() > 10) {
				s1 = s1.substring(0, 10);
			}
			if (StringUtils.isNotEmpty(s2) && s2.length() > 10) {
				s2 = s2.substring(0, 10);
			}
		}
		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
			} else {
				eq = false;
			}
		} else {
			if (StringUtils.isNotEmpty(s2)) {
				eq = false;
			}
		}
		if (!eq) {
			p1.put(key + "HasChanged", "true");
		}

		return eq;
	}

	private Timestamp getTimeStamp(Attribute attr) throws Exception {
		Timestamp ts = null;

		long adTimeUnits = Long.parseLong((String) attr.get());
		if (adTimeUnits > 0) {
			long milliseconds = (adTimeUnits / 10000) - DIFF_NET_JAVA_FOR_DATES;
			ts = new Timestamp(milliseconds);
			if (log.isDebugEnabled()) {
				log.debug("Date/Time conversion: attr=" + attr + "; adTimeUnits=" + adTimeUnits + "; milliseconds=" + milliseconds + "; ts=" + ts);
			}
		}

		return ts;
	}
 
	/**
	 * <p>Builds and returns an HTTPClient.</p>
	 *
	 * @return an HTTPClient
	 */
	private static HttpClient createHttpClient() {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		try {
			SSLContext ctx = SSLContext.getInstance("TLSv1.2");
			X509TrustManager tm = new X509TrustManager(){

				@Override
				public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}

				@Override
				public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}

				@Override
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			};
			ctx.init(null, new TrustManager[]{tm}, null);
			SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			ClientConnectionManager ccm = httpClient.getConnectionManager();
			SchemeRegistry sr = ccm.getSchemeRegistry();
			sr.register(new Scheme("https", 443, (SchemeSocketFactory)ssf));
			httpClient = new DefaultHttpClient(ccm, httpClient.getParams());
		} catch (Exception e) {
			System.out.println("Exception encountered: " + e.getClass().getName() + "; " + e.getMessage());
		}
		return httpClient;
	}

	/**
	 * @param contextFactory the providerUrl to set
	 */
	public void setContextFactory(String contextFactory) {
		this.contextFactory = contextFactory;
	}

	/**
	 * @param providerUrl the providerUrl to setcontextFactory
	 */
	public void setProviderUrl(String providerUrl) {
		this.providerUrl = providerUrl;
	}

	/**
	 * @param securityAuth the securityAuth to set
	 */
	public void setSecurityAuth(String securityAuth) {
		this.securityAuth = securityAuth;
	}

	/**
	 * @param securityPrin the securityPrin to set
	 */
	public void setSecurityPrin(String securityPrin) {
		this.securityPrin = securityPrin;
	}

	/**
	 * @param securityCred the securityCred to set
	 */
	public void setSecurityCred(String securityCred) {
		this.securityCred = securityCred;
	}

	/**
	 * @param standardSearch the standardSearch to set
	 */
	public void setStandardSearch(String standardSearch) {
		this.standardSearch = standardSearch;
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
	 * @param prDataSource the prDataSource to set
	 */
	public void setPrDataSource(DataSource prDataSource) {
		this.prDataSource = prDataSource;
	}

	/**
	 * @param iamDataSource the iamDataSource to set
	 */
	public void setIamDataSource(DataSource iamDataSource) {
		this.iamDataSource = iamDataSource;
	}

	/**
	 * @param psDataSource the psDataSource to set
	 */
	public void setPsDataSource(DataSource psDataSource) {
		this.psDataSource = psDataSource;
	}

	/**
	 * @param ppsDataSource the ppsDataSource to set
	 */
	public void setPpsDataSource(DataSource ppsDataSource) {
		this.ppsDataSource = ppsDataSource;
	}

	/**
	 * @param badgeDataSource the badgeDataSource to set
	 */
	public void setBadgeDataSource(DataSource badgeDataSource) {
		this.badgeDataSource = badgeDataSource;
	}

	/**
	 * @param portraitDataSource the portraitDataSource to set
	 */
	public void setPortraitDataSource(DataSource portraitDataSource) {
		this.portraitDataSource = portraitDataSource;
	}

	/**
	 * @param eventService the eventService to set
	 */
	public void setEventService(EventService eventService) {
		this.eventService = eventService;
	}
}
