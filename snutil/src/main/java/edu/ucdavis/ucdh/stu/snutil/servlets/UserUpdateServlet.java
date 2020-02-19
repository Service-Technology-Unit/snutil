package edu.ucdavis.ucdh.stu.snutil.servlets;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;

/**
 * <p>This servlet updates the ServiceNow sys_user table with data from the UCDH Person Repository.</p>
 */
public class UserUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_URL = "/api/now/table/sys_user?sysparm_display_value=all&sysparm_query=employee_number%3D";
	private static final String FETCH_BY_EXT_URL = "/api/now/table/sys_user?sysparm_display_value=all&sysparm_query=u_external_id%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String IDENTITY_FETCH_URL = "/api/now/table/x_ucdhs_identity_s_identity?sysparm_fields=sys_id&sysparm_query=user%3D";
	private static final String IDENTITY_UPDATE_URL = "/api/now/table/x_ucdhs_identity_s_identity/";
	private static final String BLDG_URL = "/api/now/table/cmn_building?sysparm_fields=sys_id&sysparm_query=name%3D";
	private static final String CC_URL = "/api/now/table/cmn_cost_center?sysparm_fields=sys_id&sysparm_query=account_number%3D";
	private static final String DEPT_URL = "/api/now/table/cmn_department?sysparm_fields=sys_id&sysparm_query=u_id_6%3D";
	private static final String LOC_URL = "/api/now/table/cmn_location?sysparm_fields=sys_id&sysparm_query=u_location_code%3D";
	private static final String GROUP_URL = "/api/now/table/sys_user_group?sysparm_fields=sys_id&sysparm_query=name%3D";
	private static final String GROUP_MEMBER_URL = "/api/now/table/sys_user_grmember";
	private static final String GROUP_MEMBER_FETCH_URL = "/api/now/table/sys_user_grmember?sysparm_fields=sys_id&sysparm_query=group%3D";
	private static final String UPDATE_URL = "/api/now/table/sys_user";
	private static final String PHOTO_URL = "/api/now/table/ecc_queue";
	private static final String LIVE_PROFILE_URL = "/api/now/table/live_profile";
	private static final String PHOTO_FETCH_URL = "/api/now/table/sys_user?sysparm_display_value=true&sysparm_fields=photo&sysparm_query=sys_id%3D";
	private static final String LIVE_PHOTO_FETCH_URL = "/api/now/table/live_profile?sysparm_display_value=true&sysparm_fields=photo&sysparm_query=sys_id%3D";
	private static final String ATTACHMENT_URL = "/api/now/table/sys_attachment/";
	private static final String IT_DEPT_URL = "/api/now/table/cmn_department?sysparm_query=nameSTARTSWITHIT%20%5EORnameSTARTSWITHMED%3AAcad%20%5EORnameSTARTSWITHMED%3AResearch&sysparm_fields=sys_id";
	private static final String PHOTO_EXISTS = "photo exists";
	private static final String[] PROPERTY = {"active:isActive", "building:building", "city:city", "cost_center:deptId", "department:deptId", "email:email", "employee_number:id", "first_name:firstName", "last_name:lastName", "location:locationCode", "manager:manager", "middle_name:middleName", "mobile_phone:cellNumber", "phone:phoneNumber", "state:state", "street:address", "title:title", "u_alternate_email:alternateEmail", "u_alternate_phones:alternatePhones", "u_banner_id:bannerId", "u_cube:cube", "u_end_date:endDate", "u_external_id:externalId", "u_floor:floor", "u_is_employee:isEmployee", "u_is_external:isExternal", "u_is_previous_ucdh_employee:isPreviousHsEmployee", "u_is_student:isStudent", "u_kerberos_id:kerberosId", "u_mothra_id:mothraId", "u_pager:pagerNumber", "u_pager_provider:pagerProvider", "u_pps_id:ppsId", "u_campus_pps_id:campusPpsId", "u_room:room", "u_start_date:startDate", "u_student_id:studentId", "u_supervisor:supervisor", "u_ucpath_id:ucPathId", "u_volunteer_id:volunteerId", "user_name:hsAdId", "zip:zip"};
	private static final String[] BOOLEAN_PROPERTY = {"active", "u_is_employee", "u_is_external", "u_is_previous_ucdh_employee", "u_is_student"};
	private static final String[] PERSON_PROPERTY = {"manager", "u_supervisor"};
	private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,String> referenceURL = new HashMap<String,String>();
	private Map<String,Map<String,String>> referenceCache = new HashMap<String,Map<String,String>>();
	private List<String> itDepartments = new ArrayList<String>();
	private DataSource badgeDataSource = null;
	private DataSource portraitDataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		badgeDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("badgeDataSource");
		portraitDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("portraitDataSource");
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
		referenceURL.put("group", GROUP_URL);
		referenceCache.put("group", new HashMap<String,String>());
		loadITDepartments();
	}

	/**
	 * <p>Processes the incoming request.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the request processing
	 */
	@SuppressWarnings("unchecked")
	protected String processRequest(HttpServletRequest req, JSONObject details) {
		String response = "";

		String action = (String) details.get("action");
		String iamId = (String) details.get("id");
		String sysId = null;
		if (StringUtils.isNotEmpty(iamId)) {
			JSONObject newPerson = buildPersonFromRequest(req, details);
			details.put("newData", newPerson);
			JSONObject oldPerson = fetchServiceNowUser(iamId, details);
			details.put("existingData", oldPerson);
			if (oldPerson != null) {
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
					response = "1;No action taken -- no changes detected";
				} else {
					response = updateServiceNowUser(newPerson, oldPerson, sysId, action, details);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- person not on file";
				} else {
					if ("false".equalsIgnoreCase((String) newPerson.get("active"))) {
						response = "1;No action taken -- INACTIVE persons are not inserted";
					} else {
						response = insertServiceNowUser(newPerson, details);
						sysId = (String) newPerson.get("sys_id");
						if (StringUtils.isNotEmpty(sysId)) {
							Map<String,String> cache = referenceCache.get("user");
							if (!cache.containsKey(iamId)) {
								cache.put(iamId, sysId);
							}
						}
					}
				}
			}
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
							response += addPhoto(sysId, base64PhotoData, details);
						}
						if (livePhotoNeeded) {
							response += addLivePhoto(sysId, newPerson.get("first_name") + " " + newPerson.get("last_name"), liveProfileSysId, base64PhotoData, details);
						}
					}
				}
			}
		} else {
			response = "2;Error - Required parameter \"id\" has no value";
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow user data on file, if present.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow user data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowUser(String iamId, JSONObject details) {
		JSONObject user = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow user data for IAM ID " + iamId);
		}
		String url = serviceNowServer + FETCH_URL + iamId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
				log.debug("HTTP response: " + resp);
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User fetch error", "Invalid HTTP Response Code returned when fetching user data for IAM ID " + iamId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user data for IAM ID " + iamId + ": " + rc);
				}
			}
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
				JSONObject newPerson = (JSONObject) details.get("newData");
				String externalId = (String) newPerson.get("u_external_id");
				if (StringUtils.isNotEmpty(externalId) && externalId.startsWith("H0")) {
					user = fetchServiceNowUserByExtId(externalId, iamId, details);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user with IAM ID " + iamId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for user with IAM ID " + iamId + ": " + e, details, e));
		}

		return user;
	}

	/**
	 * <p>Returns the ServiceNow user data on file, if present.</p>
	 *
	 * @param externalId the External ID of the person
	 * @param iamId the IAM ID of the person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow user data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowUserByExtId(String externalId, String iamId, JSONObject details) {
		JSONObject user = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow user data for External ID " + externalId);
		}
		String url = serviceNowServer + FETCH_BY_EXT_URL + externalId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
				log.debug("HTTP response: " + resp);
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User fetch error", "Invalid HTTP Response Code returned when fetching user data for External ID " + externalId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user data for External ID " + externalId + ": " + rc);
				}
			}
			JSONArray users = (JSONArray) result.get("result");
			if (users != null && users.size() > 0) {
				user = (JSONObject) users.get(0);
				if (log.isDebugEnabled()) {
					log.debug("User found for External ID " + externalId + ": " + user);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User not found for External ID " + externalId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user with External ID " + externalId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for user with External ID " + externalId + ": " + e, details, e));
		}

		return user;
	}

	/**
	 * <p>Returns the sys_id of the ServiceNow Live Profile, if present.</p>
	 *
	 * @param sysId the sys_id of the user
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the sys_id of the ServiceNow Live Profile, if present
	 */
	@SuppressWarnings("unchecked")
	private String checkLiveProfile(String sysId, JSONObject details) {
		String liveProfileSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow Live Profile for User sys_id " + sysId);
		}
		String url = serviceNowServer + LIVE_PROFILE_URL + "?sysparm_display_value=all&sysparm_fields=sys_id%2Cphoto&sysparm_query=document%3D" + sysId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "Live Profile fetch error", "Invalid HTTP Response Code returned when fetching Live Profile data for sys_id " + sysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching Live Profile data for sys_id " + sysId + ": " + rc);
				}
			}
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
			eventService.logEvent(new Event((String) details.get("id"), "Live Profile fetch exception", "Exception encountered searching for Live Profile with sys_id " + sysId + ": " + e, details, e));
		}

		return liveProfileSysId;
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
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowUser(JSONObject newPerson, JSONObject details) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting person " + newPerson.get("employee_number"));
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("company", ucDavisHealth);
		insertData.put("date_format", "MM-dd-yyyy");
		insertData.put("preferred_language", "English");
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!field.equals("deptName")) {
				if (StringUtils.isNotEmpty((String) newPerson.get(field))) {
					insertData.put(field, newPerson.get(field));
				}
			}
		}
		if (StringUtils.isEmpty((String) insertData.get("user_name"))) {
			if (StringUtils.isEmpty((String) insertData.get("u_external_id"))) {
				insertData.put("user_name", insertData.get("employee_number"));
			} else {
				insertData.put("user_name", insertData.get("u_external_id"));
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + insertData.toJSONString());
		}

		// post parameters
		try {
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting JSON data to " + url);
			}
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			JSONObject result = new JSONObject();
			String sysId = null;
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
					result = (JSONObject) jsonResponse.get("result");
					if (result != null) {
						sysId = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty("sys_id")) {
							newPerson.put("sys_id", sysId);
						}
					}
				}
			}
			if (rc == 200 || rc == 201) {
				response = "0;User inserted" + resolveITGroupAffiliation(newPerson, sysId, details);
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
				response = "2;Unable to insert user";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new user: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User insert error", "Invalid HTTP Response Code returned when inserting new user: " + rc, details));
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new user " + newPerson.get("employee_number") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User insert exception", "Exception occured when attempting to insert new user " + newPerson.get("employee_number") + ": " + e, details, e));
			response = "2;Unable to insert user";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param sysId the existing person's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowUser(JSONObject newPerson, JSONObject oldPerson, String sysId, String action, JSONObject details) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Updating person " + newPerson.get("employee_number"));
		}

		// create HttpPut
		String url = serviceNowServer + UPDATE_URL + "/" + sysId;
		HttpPut put = new HttpPut(url);
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		if (action.equalsIgnoreCase("delete")) {
			updateData.put("active", "false");
			updateData.put("user_name", newPerson.get("employee_number"));
			String endDate = "";
			JSONObject endDateInfo = (JSONObject) oldPerson.get("u_end_date");
			if (endDateInfo != null) {
				endDate = (String) endDateInfo.get("value");
			}
			if (StringUtils.isEmpty(endDate)) {
				updateData.put("u_end_date", df.format(new Date()));
			}
		} else {
			Iterator<String> i = fieldMap.keySet().iterator();
			while (i.hasNext()) {
				String field = i.next();
				if (!field.equals("deptName")) {
					if ("true".equalsIgnoreCase((String) newPerson.get(field + "HasChanged"))) {
						String value = (String) newPerson.get(field);
						if (StringUtils.isNotEmpty(value)) {
							updateData.put(field, value);
						}
					}
				}
			}
			String onboardingInProgress = "";
			JSONObject onboardingInfo = (JSONObject) oldPerson.get("u_onboarding_in_progress");
			if (onboardingInfo != null) {
				onboardingInProgress = (String) onboardingInfo.get("value");
			}
			if ("true".equalsIgnoreCase(onboardingInProgress)) {
				updateData.put("u_onboarding_in_progress", "false");
				updateData.put("locked_out", "false");
				updateServiceNowIdentity(sysId, (String) newPerson.get("employee_number"), details);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("JSON object to PUT: " + updateData.toJSONString());
		}

		// put JSON
		try {
			put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON update to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200) {
				response = "0;User updated" + resolveITGroupAffiliation(newPerson, sysId, details);
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update user";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc);
				}
				eventService.logEvent(new Event((String) details.get("id"), "User update error", "Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc, details));
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
			eventService.logEvent(new Event((String) details.get("id"), "User update exception", "Exception occured when attempting to update user " + newPerson.get("employee_number") + ": " + e, details, e));
			response = "2;Unable to update user";
		}

		return response;
	}

	/**
	 * <p>Updates the Identity record for pending users.</p>
	 *
	 * @param sysId the existing person's ServiceNow sys_id
	 * @param iamId the existing person's IAM ID
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 */
	@SuppressWarnings("unchecked")
	private void updateServiceNowIdentity(String sysId, String iamId, JSONObject details) {
		String idSysId = fetchIdentitySysId(sysId, details);

		if (log.isDebugEnabled()) {
			log.debug("Updating identity " + idSysId);
		}

		// create HttpPut
		String url = serviceNowServer + IDENTITY_UPDATE_URL + "/" + idSysId;
		HttpPut put = new HttpPut(url);
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		updateData.put("status", "Active");
		updateData.put("active", "true");
		updateData.put("employee_number", iamId);

		// put JSON
		try {
			put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
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
					log.debug("Invalid HTTP Response Code returned when updating identity sys_id " + idSysId + ": " + rc);
				}
				eventService.logEvent(new Event((String) details.get("id"), "Identity update error", "Invalid HTTP Response Code returned when updating identity sys_id " + idSysId + ": " + rc, details));
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
			log.debug("Exception occured when attempting to update identity " + idSysId + ": " + e);
			eventService.logEvent(new Event((String) details.get("id"), "Identity update exception", "Exception occured when attempting to update identity sys_id " + idSysId + ": " + e, details, e));
		}
	}

	/**
	 * <p>Returns the ServiceNow sys_id for the Identity associated with the User passed.</p>
	 *
	 * @param sysId the ServiceNow sys_id of the user
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the sys_id of the identity associated with the user
	 */
	private String fetchIdentitySysId(String sysId, JSONObject details) {
		String idSysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching Identity sys_id for user " + sysId);
		}

		String url = serviceNowServer + IDENTITY_FETCH_URL + sysId;
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching Identity sys_id using url " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching Identity sys_id for user " + sysId + ": " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					JSONObject obj = (JSONObject) result.get(0);
					idSysId = (String) obj.get("sys_id");
					if (StringUtils.isNotEmpty(sysId)) {
						if (log.isDebugEnabled()) {
							log.debug("sys_id found for user" + sysId + ": " + idSysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for user " + sysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for Identity sys_id for user " + sysId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Identity sys_id fetch exception", "Exception encountered searching for Identity sys_id for user " + sysId + ": " + e, details, e));
		}

		return idSysId;
	}

	/**
	 * <p>Adds or removes people from the IT Staff group based on their Department.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param sysId the existing person's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	private String resolveITGroupAffiliation(JSONObject newPerson, String sysId, JSONObject details) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Evaluating person " + newPerson.get("employee_number") + " for IT Staff affiliation.");
		}

		// check current department
		String department = (String) newPerson.get("department");
		if ("true".equalsIgnoreCase((String) newPerson.get("active")) && StringUtils.isNotEmpty(department) && itDepartments.contains(department)) {
			response = addToITStaffGroup(newPerson, sysId, details);
		} else {
			response = removeFromITStaffGroup(newPerson, sysId, details);
		}

		return response;
	}

	/**
	 * <p>Adds the user to the IT Staff group if needed.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param sysId the existing person's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String addToITStaffGroup(JSONObject newPerson, String sysId, JSONObject details) {
		String response = "";

		// check existing membership
		String membershipSysId = fetchITGroupAffiliation(sysId, details);
		if (StringUtils.isNotEmpty(membershipSysId)) {
			if (log.isDebugEnabled()) {
				log.debug("Employee " + newPerson.get("employee_number") + " is already a member of the IT Staff group.");
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Adding Employee " + newPerson.get("employee_number") + " to the IT Staff group.");
			}

			// create HttpPost
			String url = serviceNowServer + GROUP_MEMBER_URL;
			HttpPost post = new HttpPost(url);
			post.setHeader(HttpHeaders.ACCEPT, "application/json");
			post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

			// build JSON to post
			JSONObject updateData = new JSONObject();
			updateData.put("user", sysId);
			updateData.put("group", getReferenceSysId("group", "IT Staff", details));
			if (log.isDebugEnabled()) {
				log.debug("JSON object to POST: " + updateData.toJSONString());
			}

			// post JSON
			try {
				post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
				post.setEntity(new StringEntity(updateData.toJSONString()));
				HttpClient client = HttpClientProvider.getClient();
				if (log.isDebugEnabled()) {
					log.debug("Posting JSON update to " + url);
				}
				HttpResponse resp = client.execute(post);
				int rc = resp.getStatusLine().getStatusCode();
				if (rc == 200 || rc == 201) {
					response = "; User added to IT Staff group";
					if (log.isDebugEnabled()) {
						log.debug("HTTP response code from post: " + rc);
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Invalid HTTP Response Code returned when attempting to add user " + sysId + " to IT Staff group: " + rc);
					}
					eventService.logEvent(new Event((String) details.get("id"), "User update error", "Invalid HTTP Response Code returned when attempting to add user " + sysId + " to IT Staff group: " + rc, details));
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
				log.debug("Exception occured when attempting to add user " + sysId + " to IT Staff group " + e);
				eventService.logEvent(new Event((String) details.get("id"), "User update exception", "Exception occured when attempting to add user " + sysId + " to IT Staff group: " + e, details, e));
			}
		}

		return response;
	}

	/**
	 * <p>Removes the user from the IT Staff group if needed.</p>
	 *
	 * @param newPerson the new data for this person
	 * @param sysId the existing person's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	private String removeFromITStaffGroup(JSONObject newPerson, String sysId, JSONObject details) {
		String response = "";

		// check existing membership
		String membershipSysId = fetchITGroupAffiliation(sysId, details);
		if (StringUtils.isNotEmpty(membershipSysId)) {
			if (log.isDebugEnabled()) {
				log.debug("Removing Employee " + newPerson.get("employee_number") + " from the IT Staff group.");
			}

			// create HttpDelete
			String url = serviceNowServer + GROUP_MEMBER_URL + "/" + membershipSysId;
			HttpDelete delete = new HttpDelete(url);
			delete.setHeader(HttpHeaders.ACCEPT, "application/json");
			delete.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

			// delete
			try {
				delete.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), delete, null));
				HttpClient client = HttpClientProvider.getClient();
				if (log.isDebugEnabled()) {
					log.debug("Issuing HTTP DELETE to URL " + url);
				}
				HttpResponse resp = client.execute(delete);
				int rc = resp.getStatusLine().getStatusCode();
				if (rc == 200 || rc == 204) {
					response = "; User removed from IT Staff group";
					if (log.isDebugEnabled()) {
						log.debug("HTTP response code from delete: " + rc);
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Invalid HTTP Response Code returned when attempting to remove user " + sysId + " from IT Staff group: " + rc);
					}
					eventService.logEvent(new Event((String) details.get("id"), "User update error", "Invalid HTTP Response Code returned when attempting to remove user " + sysId + " from IT Staff group: " + rc, details));
				}
				if (log.isDebugEnabled()) {
					String jsonRespString = "";
					HttpEntity entity = resp.getEntity();
					if (entity != null) {
						jsonRespString = EntityUtils.toString(entity);
						JSONObject result = (JSONObject) JSONValue.parse(jsonRespString);
						log.debug("JSON response: " + result.toJSONString());
					}
				}
			} catch (Exception e) {
				log.debug("Exception occured when attempting to remove user " + sysId + " from IT Staff group " + e);
				eventService.logEvent(new Event((String) details.get("id"), "User update exception", "Exception occured when attempting to remove user " + sysId + " from IT Staff group: " + e, details, e));
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Employee " + newPerson.get("employee_number") + " is not a member of the IT Staff group.");
			}
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the IT Staff group membership for the passed user.</p>
	 *
	 * @param userSysId the ServiceNow sys_id for the person for whom the membership is being requested
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the IT Staff group membership for the passed user
	 */
	private String fetchITGroupAffiliation(String userSysId, JSONObject details) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for IT Staff group membership for " + userSysId);
		}

		String url = serviceNowServer + GROUP_MEMBER_FETCH_URL + getReferenceSysId("group", "IT Staff", details) + "%5Euser%3D" + userSysId;
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id for IT Staff group membership using url " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for IT Staff group membership for user " + userSysId + ": " + rc);
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
							log.debug("sys_id found for IT Staff group membership for user " + userSysId + ": " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for IT Staff group membership for user " + userSysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for IT Staff group membership for user " + userSysId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User sys_id fetch exception", "Exception encountered searching for sys_id for IT Staff group membership for user " + userSysId + ": " + e, details, e));
		}

		return sysId;
	}

	/**
	 * <p>Builds a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the person data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildPersonFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject person = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			person.put(target, req.getParameter(source));
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
				person.put(field, getUserSysId((String) person.get(field), details));
			}
		}

		i = referenceURL.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) person.get(field))) {
				person.put(field, getReferenceSysId(field, (String) person.get(field), details));
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

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the person for whom the sys_id is being requested
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the iamId passed
	 */
	private String getUserSysId(String iamId, JSONObject details) {
		String sysId = null;

		Map<String,String> cache = referenceCache.get("user");
		if (cache.containsKey(iamId)) {
			sysId = cache.get(iamId);
		} else {
			if (StringUtils.isNotEmpty(iamId) && iamId.indexOf(" ") == -1) {
				sysId = fetchUserSysId(iamId, details);
				if (StringUtils.isNotEmpty(sysId)) {
					cache.put(iamId, sysId);
				}
			} else {
				cache.put(iamId, "");
				if (log.isDebugEnabled()) {
					log.debug("Will not search for user sys_id with invalid IAM ID: " + iamId);
				}
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
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the iamId passed
	 */
	private String getReferenceSysId(String field, String value, JSONObject details) {
		String returnValue = null;

		Map<String,String> cache = referenceCache.get(field);
		if (cache.containsKey(value)) {
			returnValue = cache.get(value);
		} else {
			returnValue = fetchReferenceSysId(field, value, details);
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
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the iamId passed
	 */
	private String fetchUserSysId(String iamId, JSONObject details) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for IAM ID " + iamId);
		}

		String url = serviceNowServer + SYSID_URL + iamId;
		try {
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
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
			eventService.logEvent(new Event((String) details.get("id"), "User sys_id fetch exception", "Exception encountered searching for sys_id for IAM ID " + iamId + ": " + e, details, e));
		}

		return sysId;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the information passed.</p>
	 *
	 * @param field the field for which the sys_id is being requested
	 * @param value the field value for which the sys_id is being requested
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the information passed
	 */
	private String fetchReferenceSysId(String field, String value, JSONObject details) {
		String sysId = "";

		try {
			String url = serviceNowServer + referenceURL.get(field) + URLEncoder.encode(value, "UTF-8");
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id for " + field + " " + value + " from URL " + url);
			}
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
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
			eventService.logEvent(new Event((String) details.get("id"), field + " fetch exception", "Exception encountered searching for sys_id for " + field + " " + value + ": " + e, details, e));
		}

		return sysId;
	}

	/**
	 * <p>Loads the list of ServiceNow sys_ids for all IT departments.</p>
	 *
	 */
	private String loadITDepartments() {
		String sysId = "";

		try {
			String url = serviceNowServer + IT_DEPT_URL;
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_ids for all IT departments from URL " + url);
			}
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_ids for all IT departments: " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					for (int i=0; i<result.size(); i++) {
						JSONObject obj = (JSONObject) result.get(i);
						sysId = (String) obj.get("sys_id");
						if (StringUtils.isNotEmpty(sysId)) {
							itDepartments.add(sysId);
							if (log.isDebugEnabled()) {
								log.debug("Adding IT department sys_id: " + sysId);
							}
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_ids not found for any IT department");
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_ids for all IT departments: " + e, e);
			eventService.logEvent(new Event("[none]", "sys_id fetch exception", "Exception encountered searching for sys_ids for all IT departments: " + e, null, e));
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
		String ucPathId = newPerson.get("u_ucpath_id");
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
			if (StringUtils.isEmpty(photoId) && StringUtils.isNotEmpty(ucPathId)) {
				rs.close();
				ps.close();
				if (log.isDebugEnabled()) {
					log.debug("photoId not found using iamId: " + iamId + "; trying again using ucPathId: " + ucPathId);
				}
				ps = conn.prepareStatement("select c_id from cardholder where c_nick_name=?");
				ps.setString(1, ucPathId);
				rs = ps.executeQuery();
				if (rs.next()) {
					photoId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("photoId from ucPathId: " + photoId);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing photoId: " + e.getMessage(), e);
			eventService.logEvent(new Event((String) details.get("id"), "Photo ID fetch exception", "Exception encountered accessing photoId: " + e.getMessage(), details, e));
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
			eventService.logEvent(new Event((String) details.get("id"), "Photo processing exception", "Exception encountered processing image data: " + e.getMessage(), details, e));
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
	 *  <p>Adds the user's photo</p>
	 *
	 * @param sysId the sys_id of the user
	 * @param base64PhotoData the user's photo in base64 encoded format
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the attempt to add the photo
	 */
	@SuppressWarnings("unchecked")
	private String addPhoto(String sysId, String base64PhotoData, JSONObject details) {
		String outcome = "";

		if (log.isDebugEnabled()) {
			log.debug("Adding photo to sys_id " + sysId);
		}
		// create HttpPost
		String url = serviceNowServer + PHOTO_URL;
		HttpPost post = new HttpPost(url);
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
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting XML photo request to " + url);
			}
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (rc == 200 || rc == 201) {
				outcome = "; photo added to user profile";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				outcome = "; unable to add user photo due to error";
				details.put("responseCode", rc + "");
				details.put("responseBody", json);
				eventService.logEvent(new Event((String) details.get("id"), "Photo posting error", "Invalid HTTP Response Code returned when posting photo for sys_id " + sysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when posting photo for sys_id " + sysId + ": " + rc);
				}
			}
			if (json != null) {
				JSONObject result = (JSONObject) json.get("result");
				if (result != null) {
					String error = (String) result.get("error_string");
					if (StringUtils.isNotEmpty(error)) {
						outcome = "; unable to add user photo due to error";
						log.error("Error attempting to add user photo: " + error);
						details.put("responseCode", rc + "");
						details.put("responseBody", json);
						eventService.logEvent(new Event((String) details.get("id"), "Photo posting error", "Error attempting to add user photo: " + error, details));
					} else {
						if (log.isDebugEnabled()) {
							log.debug(result.get("payload"));
						}
						String userPhotoSysId = fetchUserPhotoSysId(sysId, details);
						if (StringUtils.isNotEmpty(userPhotoSysId)) {
							updateAttachment(userPhotoSysId, "sys_user", details);
						} else {
							log.error("Unable to locate attachment for user photo for user " + sysId);
							details.put("responseCode", rc + "");
							details.put("responseBody", json);
							eventService.logEvent(new Event((String) details.get("id"), "Photo posting error", "Unable to locate attachment for user photo for user " + sysId, details));
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occured when posting photo for sys_id " + sysId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Photo posting exception", "Exception occured when posting photo for sys_id " + sysId + ": " + e, details, e));
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
	@SuppressWarnings("unchecked")
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
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting XML Live Profile photo request to " + url);
			}
			HttpResponse response = client.execute(post);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (rc == 200 || rc == 201) {
				outcome = "; photo added to user's Live Profile";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				outcome = "; unable to add user's Live Profile photo due to error";
				details.put("responseCode", rc + "");
				details.put("responseBody", json);
				eventService.logEvent(new Event((String) details.get("id"), "Photo posting error", "Invalid HTTP Response Code returned when posting Live Profile photo for sys_id " + sysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when posting Live Profile photo for sys_id " + sysId + ": " + rc);
				}
			}
			if (json != null) {
				JSONObject result = (JSONObject) json.get("result");
				if (result != null) {
					String error = (String) result.get("error_string");
					if (StringUtils.isNotEmpty(error)) {
						outcome = "; unable to add user's Live Profile photo due to error";
						log.error("Error attempting to add user's Live Profile photo: " + error);
						details.put("responseCode", rc + "");
						details.put("responseBody", json);
						eventService.logEvent(new Event((String) details.get("id"), "Photo posting error", "Error attempting to add user's Live Profile photo: " + error, details));
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
			eventService.logEvent(new Event((String) details.get("id"), "Photo posting exception", "Exception occured when posting Live Profile photo for sys_id " + sysId + ": " + e, details, e));
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
			put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
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
				eventService.logEvent(new Event((String) details.get("id"), "Attachment update error", "Invalid HTTP Response Code returned when updating attachment " + attachmentSysId + " for table " + table + ": " + rc, details));
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
			eventService.logEvent(new Event((String) details.get("id"), "Attachment update exception", "Exception occured when attempting to update attachment " + attachmentSysId + " for table " + table + ": " + e, details, e));
		}
	}

	/**
	 * <p>Returns the sys_id of the user photo, if present.</p>
	 *
	 * @param sysId the sys_id of the user
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return  the sys_id of the user photo, if present
	 */
	@SuppressWarnings("unchecked")
	private String fetchUserPhotoSysId(String sysId, JSONObject details) {
		String userPhotoSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching user photo sys_id for user Profile " + sysId);
		}
		String url = serviceNowServer + PHOTO_FETCH_URL + sysId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User fetch error", "Invalid HTTP Response Code returned when fetching user photo sys_id for user " + sysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user photo sys_id for user " + sysId + ": " + rc);
				}
			}
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
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for user photo sys_id for user " + sysId + ": " + e, details, e));
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
	@SuppressWarnings("unchecked")
	private String fetchLiveProfilePhotoSysId(String liveProfileSysId, JSONObject details) {
		String livePhotoSysId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId);
		}
		String url = serviceNowServer + LIVE_PHOTO_FETCH_URL + liveProfileSysId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User fetch error", "Invalid HTTP Response Code returned when fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + rc);
				}
			}
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
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for Live Profile Photo sys_id for Live Profile " + liveProfileSysId + ": " + e, details, e));
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
	@SuppressWarnings("unchecked")
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
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new ByteArrayEntity(xml.getBytes("UTF-8")));
			HttpClient client = HttpClientProvider.getClient();
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
						details.put("responseCode", rc + "");
						details.put("responseBody", json);
						eventService.logEvent(new Event((String) details.get("id"), "Thumbnail posting error", "Error attempting to add user's Live Profile thumbnail: " + error, details));
					} else {
						if (log.isDebugEnabled()) {
							log.debug(result.get("payload"));
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occured when posting thumbnail for Live Profile photo " + livePhotoSysId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Thumbnail posting exception", "Exception occured when posting thumbnail for Live Profile photo " + livePhotoSysId + ": " + e, details, e));
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
			eventService.logEvent(new Event((String) details.get("id"), "Thumbnail posting exception", "Exception occured when creating thumbnail image fron user photo: " + e, details, e));
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
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
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
			eventService.logEvent(new Event((String) details.get("id"), "User insert exception", "Exception occured when attempting to insert Live Profile for " + name + ": " + e, details, e));
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
}
