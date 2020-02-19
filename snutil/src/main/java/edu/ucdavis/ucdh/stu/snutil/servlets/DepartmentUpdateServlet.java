package edu.ucdavis.ucdh.stu.snutil.servlets;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;

/**
 * <p>This servlet updates the ServiceNow cmn_department table with data from the UCDH Department Repository.</p>
 */
public class DepartmentUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_URL = "/api/now/table/cmn_department?sysparm_display_value=all&sysparm_query=id%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String UPDATE_URL = "/api/now/table/cmn_department";
	private static final String CC_URL = "/api/now/table/cmn_cost_center?sysparm_fields=sys_id&sysparm_query=account_number%3D";
	private static final String[] PROPERTY = {"cost_center:id", "dept_head:manager", "description:name", "id:id", "name:name", "primary_contact:manager", "u_active:isActive", "u_id_4:costCenterId", "u_id_6:alternateId"};
	private static final String[] PERSON_PROPERTY = {"dept_head", "primary_contact"};
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,String> referenceURL = new HashMap<String,String>();
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
		referenceURL.put("cost_center", CC_URL);
		referenceCache.put("cost_center", new HashMap<String,String>());
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
			JSONObject newDepartment = buildDepartmentFromRequest(req, details);
			details.put("newData", newDepartment);
			JSONObject oldDepartment = fetchServiceNowDepartment(id, details);
			details.put("existingData", oldDepartment);
			if (oldDepartment != null) {
				if (departmentUnchanged(newDepartment, oldDepartment)) {
					response = "1;No action taken -- no changes detected";
				} else {
					response = updateServiceNowDepartment(newDepartment, oldDepartment);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- department not on file";
				} else {
					if ("false".equalsIgnoreCase((String) newDepartment.get("u_active"))) {
						response = "1;No action taken -- INACTIVE departments are not inserted";
					} else {
						response = insertServiceNowDepartment(newDepartment);
					}
				}
			}
		} else {
			response = "2;Error - Required parameter \"id\" has no value";
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow department data on file, if present.</p>
	 *
	 * @param id the ID of the department
	 * @return the ServiceNow department data
	 */
	private JSONObject fetchServiceNowDepartment(String id, JSONObject details) {
		JSONObject department = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow department data for ID " + id);
		}
		try {
			String url = serviceNowServer + FETCH_URL + URLEncoder.encode(id, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching department data using url " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching department data for ID " + id + ": " + rc);
					eventService.logEvent(new Event((String) details.get("id"), "Department fetch error", "Invalid HTTP Response Code returned when fetching department data for ID " + id + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray departments = (JSONArray) result.get("result");
			if (departments != null && departments.size() > 0) {
				department = (JSONObject) departments.get(0);
				if (log.isDebugEnabled()) {
					log.debug("Department found for ID " + id + ": " + department);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Department not found for ID " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for department with ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Department fetch exception", "Exception encountered searching for department with ID " + id + ": " + e, details, e));
		}

		return department;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newDepartment the new data for this department
	 * @param oldDepartment the existing data for this department
	 * @return true if the department is unchanged
	 */
	private boolean departmentUnchanged(JSONObject newDepartment, JSONObject oldDepartment) {
		boolean unchanged = true;

		if ("false".equalsIgnoreCase((String) newDepartment.get("u_active")) && "false".equalsIgnoreCase((String) ((JSONObject) oldDepartment.get("u_active")).get("value"))) {
			// inactive departments are not updated
		} else {
			Iterator<String> i = fieldMap.keySet().iterator();
			while (i.hasNext()) {
				String field = i.next();
				if (!isEqual(newDepartment, oldDepartment, field)) {
					unchanged = false;
				}
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newDepartment the new data for this department
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowDepartment(JSONObject newDepartment) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting department " + newDepartment.get("employee_number"));
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("company", ucDavisHealth);
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) newDepartment.get(field))) {
				insertData.put(field, newDepartment.get(field));
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
			if (rc == 200 || rc == 201) {
				response = "0;Department inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				response = "2;Unable to insert department";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new department: " + rc);
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
							newDepartment.put("sys_id", sysId);
							if (log.isDebugEnabled()) {
								log.debug("sys_id for inserted department: " + sysId);
							}
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + jsonRespString);
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new department " + newDepartment.get("id") + ": " + e);
			response = "2;Unable to insert ";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newDepartment the new data for this department
	 * @param sysId the existing department's ServiceNow sys_id
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowDepartment(JSONObject newDepartment, JSONObject oldDepartment) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Updating department " + newDepartment.get("id"));
		}

		// create HttpPut
		String sysId = (String) ((JSONObject) oldDepartment.get("sys_id")).get("value");
		String url = serviceNowServer + UPDATE_URL + "/" + sysId;
		HttpPut put = new HttpPut(url);
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if ("true".equalsIgnoreCase((String) newDepartment.get(field + "HasChanged"))) {
				String value = (String) newDepartment.get(field);
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
			put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON update to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			if (rc == 200) {
				response = "0;Department updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update department";
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
			log.debug("Exception occured when attempting to update department " + newDepartment.get("id") + ": " + e);
			response = "2;Unable to update department";
		}

		return response;
	}

	/**
	 * <p>Builds a new department using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the department data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildDepartmentFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject department = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			department.put(target, req.getParameter(source));
		}

		for (int x=0; x<PERSON_PROPERTY.length; x++) {
			String field = PERSON_PROPERTY[x];
			if (StringUtils.isNotEmpty((String) department.get(field))) {
				department.put(field, getUserSysId((String) department.get(field), details));
			}
		}
		String field = "cost_center";
		if (StringUtils.isNotEmpty((String) department.get(field))) {
			department.put(field, getReferenceSysId(field, (String) department.get(field), details));
		}
		if ("Y".equalsIgnoreCase((String) department.get("u_active"))) {
			department.put("u_active", "true");
		} else {
			department.put("u_active", "false");
		}

		if (log.isDebugEnabled()) {
			log.debug("Returning new department values: " + department);
		}

		return department;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the id passed.</p>
	 *
	 * @param id the ID for the department for whom the sys_id is being requested
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
	 * @param id the ID for the department for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the id passed
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
	 * <p>Returns the ServiceNow sys_id associated with the id passed.</p>
	 *
	 * @param id the ID for the department for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the id passed
	 */
	private String fetchUserSysId(String id, JSONObject details) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for ID " + id);
		}

		String url = serviceNowServer + SYSID_URL + id;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
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
			eventService.logEvent(new Event((String) details.get("id"), "Department sys_id fetch exception", "Exception encountered searching for sys_id for ID " + id + ": " + e, details, e));
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
