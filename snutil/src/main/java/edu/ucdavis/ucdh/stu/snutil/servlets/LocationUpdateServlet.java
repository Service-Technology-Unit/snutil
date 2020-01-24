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
 * <p>This servlet updates the ServiceNow cmn_location table with data from the UCDH Location Repository.</p>
 */
public class LocationUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_URL = "/api/now/table/cmn_location?sysparm_display_value=all&sysparm_query=id%3D";
	private static final String UPDATE_URL = "/api/now/table/cmn_location";
	private static final String DEPT_URL = "/api/now/table/cmn_department?sysparm_fields=sys_id%2Cdept_head&sysparm_query=id%3D";
	private static final String[] PROPERTY = {"city:city", "company:company", "contact:contact", "country:country", "latitude:latitude", "longitude:longitude", "name:fullName", "parent:parent", "state:state", "street:address", "u_department:department", "u_location_code:id", "zip:zip"};
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,String> referenceURL = new HashMap<String,String>();
	private Map<String,Map<String,JSONObject>> referenceCache = new HashMap<String,Map<String,JSONObject>>();

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
		referenceURL.put("u_department", DEPT_URL);
		referenceCache.put("u_department", new HashMap<String,JSONObject>());
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
			JSONObject newLocation = buildLocationFromRequest(req, details);
			details.put("newData", newLocation);
			JSONObject oldLocation = fetchServiceNowLocation(id, details);
			details.put("existingData", oldLocation);
			if (oldLocation != null) {
				if (locationUnchanged(newLocation, oldLocation)) {
					response = "1;No action taken -- no changes detected";
				} else {
					response = updateServiceNowLocation(newLocation, oldLocation);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- location not on file";
				} else {
					response = insertServiceNowLocation(newLocation);
				}
			}
		} else {
			response = "2;Error - Required parameter \"id\" has no value";
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow location data on file, if present.</p>
	 *
	 * @param id the ID of the location
	 * @return the ServiceNow location data
	 */
	private JSONObject fetchServiceNowLocation(String id, JSONObject details) {
		JSONObject location = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow location data for ID " + id);
		}
		try {
			String url = serviceNowServer + FETCH_URL + URLEncoder.encode(id, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching location data using url " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching location data for ID " + id + ": " + rc);
					eventService.logEvent(new Event((String) details.get("id"), "Location fetch error", "Invalid HTTP Response Code returned when fetching location data for ID " + id + ": " + rc, details));
				}
			}
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			JSONArray locations = (JSONArray) result.get("result");
			if (locations != null && locations.size() > 0) {
				location = (JSONObject) locations.get(0);
				if (log.isDebugEnabled()) {
					log.debug("Location found for ID " + id + ": " + location);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Location not found for ID " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for location with ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Location fetch exception", "Exception encountered searching for location with ID " + id + ": " + e, details, e));
		}

		return location;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newLocation the new data for this location
	 * @param oldLocation the existing data for this location
	 * @return true if the location is unchanged
	 */
	private boolean locationUnchanged(JSONObject newLocation, JSONObject oldLocation) {
		boolean unchanged = true;

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!isEqual(newLocation, oldLocation, field)) {
				unchanged = false;
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newLocation the new data for this location
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowLocation(JSONObject newLocation) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting location " + newLocation.get("employee_number"));
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("company", ucDavisHealth);
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) newLocation.get(field))) {
				insertData.put(field, newLocation.get(field));
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
				response = "0;Location inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				response = "2;Unable to insert location";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new location: " + rc);
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
							newLocation.put("sys_id", sysId);
							if (log.isDebugEnabled()) {
								log.debug("sys_id for inserted location: " + sysId);
							}
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + jsonRespString);
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new location " + newLocation.get("id") + ": " + e);
			response = "2;Unable to insert ";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newLocation the new data for this location
	 * @param sysId the existing location's ServiceNow sys_id
	 * @return the response
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowLocation(JSONObject newLocation, JSONObject oldLocation) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Updating location " + newLocation.get("id"));
		}

		// create HttpPut
		String sysId = (String) ((JSONObject) oldLocation.get("sys_id")).get("value");
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
			if ("true".equalsIgnoreCase((String) newLocation.get(field + "HasChanged"))) {
				String value = (String) newLocation.get(field);
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
				response = "0;Location updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update location";
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
			log.debug("Exception occured when attempting to update location " + newLocation.get("id") + ": " + e);
			response = "2;Unable to update location";
		}

		return response;
	}

	/**
	 * <p>Builds a new location using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the location data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildLocationFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject location = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			location.put(target, req.getParameter(source));
		}
		String field = "u_department";
		if (StringUtils.isNotEmpty((String) location.get(field))) {
			JSONObject deptInfo = getReferenceData(field, (String) location.get(field), details);
			if (deptInfo != null) {
				location.put(field, deptInfo.get("sys_id"));
				JSONObject deptHead = (JSONObject) deptInfo.get("dept_head");
				if (deptHead != null) {
					location.put("contact", deptHead.get("value"));
				}
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Returning new location values: " + location);
		}

		return location;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the id passed.</p>
	 *
	 * @param id the ID for the location for whom the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the id passed
	 */
	private JSONObject getReferenceData(String field, String value, JSONObject details) {
		JSONObject returnValue = null;

		Map<String,JSONObject> cache = referenceCache.get(field);
		if (cache.containsKey(value)) {
			returnValue = cache.get(value);
		} else {
			returnValue = fetchReferenceData(field, value, details);
			if (returnValue != null) {
				cache.put(value, returnValue);
			}
		}
		if (log.isDebugEnabled() && returnValue != null) {
			log.debug("Returning " + returnValue.toJSONString() + " for " + field + " " + value);
		}

		return returnValue;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the information passed.</p>
	 *
	 * @param field the field for which the sys_id is being requested
	 * @param value the field value for which the sys_id is being requested
	 * @return the ServiceNow sys_id associated with the information passed
	 */
	private JSONObject fetchReferenceData(String field, String value, JSONObject details) {
		JSONObject data = null;

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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow data for " + field + " " + value + ": " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					data = (JSONObject) result.get(0);
					if (data != null) {
						if (log.isDebugEnabled()) {
							log.debug("data found for " + field + " " + value + ": " + data.toJSONString());
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (data == null) {
					log.debug("data not found for " + field + " " + value);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for " + field + " " + value + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), field + " fetch exception", "Exception encountered searching for sys_id for " + field + " " + value + ": " + e, details, e));
		}

		return data;
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
