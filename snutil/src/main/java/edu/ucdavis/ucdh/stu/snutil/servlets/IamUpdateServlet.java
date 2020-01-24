package edu.ucdavis.ucdh.stu.snutil.servlets;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;

/**
 * <p>This servlet updates the ServiceNow u_iam_person_repository table with data from the IAM Person Repository.</p>
 */
public class IamUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_URL = "/api/now/table/u_iam_person_repository?sysparm_display_value=all&sysparm_query=u_iam_id%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String UPDATE_URL = "/api/now/table/u_iam_person_repository";
	private static final String[] PROPERTY = {"u_banner_id:bannerId","u_campus_id:kerberosId","u_email:email","u_external_id:externalId","u_first_name:firstName","u_health_ad:adId","u_iam_id:id","u_last_name:lastName","u_middle_name:middleName","u_pps_id:ppsId","u_student_id:studentId","u_ucpath_id:ucPathId", "u_ucpath_institution:ucPathInstitution"};
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,Map<String,String>> referenceCache = new HashMap<String,Map<String,String>>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		for (int i=0;i<PROPERTY.length;i++) {
			String[] parts = PROPERTY[i].split(":");
			fieldMap.put(parts[0], parts[1]);
		}
		referenceCache.put("user", new HashMap<String,String>());
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
		String id = (String) details.get("id");
		if (StringUtils.isNotEmpty(id)) {
			JSONObject newIdentity = buildIdentityFromRequest(req, details);
			details.put("newData", newIdentity);
			JSONObject oldIdentity = fetchServiceNowIdentity(id, details);
			details.put("existingData", oldIdentity);
			if (oldIdentity != null) {
				if (identityUnchanged(newIdentity, oldIdentity)) {
					response = "1;No action taken -- no changes detected";
				} else {
					response = updateServiceNowIdentity(newIdentity, oldIdentity);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- identity not on file";
				} else {
					response = insertServiceNowIdentity(newIdentity);
				}
			}
		} else {
			response = "2;Error - Required parameter \"id\" has no value";
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow identity data on file, if present.</p>
	 *
	 * @param id the ID of the identity
	 * @return the ServiceNow identity data
	 */
	private JSONObject fetchServiceNowIdentity(String id, JSONObject details) {
		JSONObject identity = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow identity data for ID " + id);
		}
		try {
			String url = serviceNowServer + FETCH_URL + URLEncoder.encode(id, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching identity data using url " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching identity data for ID " + id + ": " + rc);
					eventService.logEvent(new Event((String) details.get("id"), "Identity fetch error", "Invalid HTTP Response Code returned when fetching identity data for ID " + id + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray identitys = (JSONArray) result.get("result");
			if (identitys != null && identitys.size() > 0) {
				identity = (JSONObject) identitys.get(0);
				if (log.isDebugEnabled()) {
					log.debug("Identity found for ID " + id + ": " + identity);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Identity not found for ID " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for identity with ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Identity fetch exception", "Exception encountered searching for identity with ID " + id + ": " + e, details, e));
		}

		return identity;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newIdentity the new data for this identity
	 * @param oldIdentity the existing data for this identity
	 * @return true if the identity is unchanged
	 */
	private boolean identityUnchanged(JSONObject newIdentity, JSONObject oldIdentity) {
		boolean unchanged = true;

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!isEqual(newIdentity, oldIdentity, field)) {
				unchanged = false;
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newIdentity the new data for this identity
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowIdentity(JSONObject newIdentity) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting identity " + newIdentity.get("employee_number"));
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("u_full_name_id",  newIdentity.get("u_full_name_id"));
		if (StringUtils.isNotEmpty((String) newIdentity.get("u_user"))) {
			insertData.put("u_user",  newIdentity.get("u_user"));
		}
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) newIdentity.get(field))) {
				insertData.put(field, newIdentity.get(field));
			}
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
				response = "0;Identity inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				response = "2;Unable to insert identity";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new identity: " + rc);
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
							newIdentity.put("sys_id", sysId);
							if (log.isDebugEnabled()) {
								log.debug("sys_id for inserted identity: " + sysId);
							}
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + jsonRespString);
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new identity " + newIdentity.get("id") + ": " + e);
			response = "2;Unable to insert ";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newIdentity the new data for this identity
	 * @param sysId the existing identity's ServiceNow sys_id
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowIdentity(JSONObject newIdentity, JSONObject oldIdentity) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Updating identity " + newIdentity.get("id"));
		}

		// create HttpPut
		String sysId = (String) ((JSONObject) oldIdentity.get("sys_id")).get("value");
		String url = serviceNowServer + UPDATE_URL + "/" + sysId;
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		updateData.put("u_full_name_id",  newIdentity.get("u_full_name_id"));
		if (StringUtils.isNotEmpty((String) newIdentity.get("u_user"))) {
			updateData.put("u_user",  newIdentity.get("u_user"));
		}
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if ("true".equalsIgnoreCase((String) newIdentity.get(field + "HasChanged"))) {
				String value = (String) newIdentity.get(field);
				if (StringUtils.isNotEmpty(value)) {
					updateData.put(field, value);
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
				response = "0;Identity updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update identity";
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
			log.debug("Exception occured when attempting to update identity " + newIdentity.get("id") + ": " + e);
			response = "2;Unable to update identity";
		}

		return response;
	}

	/**
	 * <p>Builds a new identity using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the identity data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildIdentityFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject identity = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			identity.put(target, req.getParameter(source));
		}
		identity.put("u_user", getUserSysId((String) identity.get("u_iam_id"), details));
		identity.put("u_full_name_id", identity.get("u_first_name") + " " + identity.get("u_last_name") + " " + identity.get("u_iam_id"));

		if (log.isDebugEnabled()) {
			log.debug("Returning new identity values: " + identity);
		}

		return identity;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the id passed.</p>
	 *
	 * @param id the ID for the identity for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the id passed
	 */
	private String getUserSysId(String id, JSONObject details) {
		String sysId = null;

		Map<String,String> cache = referenceCache.get("user");
		if (cache.containsKey(id)) {
			sysId = cache.get(id);
		} else {
			sysId = fetchUserSysId(id, details);
			if (StringUtils.isNotEmpty(sysId)) {
				cache.put(id, sysId);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning sys_id " + sysId + " for ID " + id);
		}

		return sysId;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the id passed.</p>
	 *
	 * @param id the ID for the identity for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the id passed
	 */
	private String fetchUserSysId(String id, JSONObject details) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for ID " + id);
		}

		String url = serviceNowServer + SYSID_URL + id;
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for ID " + id + ": " + rc);
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
							log.debug("sys_id found for ID " + id + ": " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for ID " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Identity sys_id fetch exception", "Exception encountered searching for sys_id for ID " + id + ": " + e, details, e));
		}

		return sysId;
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