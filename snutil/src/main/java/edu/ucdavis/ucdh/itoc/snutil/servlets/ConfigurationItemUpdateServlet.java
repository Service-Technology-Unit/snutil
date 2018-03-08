package edu.ucdavis.ucdh.itoc.snutil.servlets;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
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

import edu.ucdavis.ucdh.itoc.snutil.beans.Event;

/**
 * <p>This servlet updates the ServiceNow sys_user table with data from the UCDH CI Repository.</p>
 */
public class ConfigurationItemUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_PC_URL = "/api/now/table/cmdb_ci_pc_hardware?sysparm_display_value=all&sysparm_query=name%3D";
	private static final String FETCH_SERVER_URL = "/api/now/table/cmdb_ci_server?sysparm_display_value=all&sysparm_query=name%3D";
	private static final String FETCH_PRINTER_URL = "/api/now/table/cmdb_ci_printer?sysparm_display_value=all&sysparm_query=name%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=user_name%3D";
	private static final String DEPT_URL = "/api/now/table/cmn_department?sysparm_fields=sys_id,cost_center&sysparm_query=id%3D";
	private static final String LOC_URL = "/api/now/table/cmn_location?sysparm_fields=sys_id&sysparm_query=u_location_code%3D";
	private static final String UPDATE_PC_URL = "/api/now/table/cmdb_ci_pc_hardware";
	private static final String UPDATE_SERVER_URL = "/api/now/table/cmdb_ci_server";
	private static final String UPDATE_PRINTER_URL = "/api/now/table/cmdb_ci_printer";
	private static final String[] PROPERTY = {"asset_tag:assetTag", "approval_group:assignment", "assigned_to:contactName", "assignment_group:assignment", "comments:comments", "default_gateway:defaultGateway", "department:department", "dns_domain:domain", "hardware_status:status", "ip_address:ipAddress", "location:location", "mac_address:macAddress", "manufacturer:manufacturer", "model_number:model", "model_id:model", "name:name", "os:operatingSystem", "serial_number:serialNumber", "short_description:description", "support_group:assignment", "vendor:vendor"};
	private static final String[] PERSON_PROPERTY = {"assigned_to"};
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
		referenceURL.put("department", DEPT_URL);
		referenceCache.put("department", new HashMap<String,String>());
		referenceURL.put("location", LOC_URL);
		referenceCache.put("location", new HashMap<String,String>());
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
		String type = req.getParameter("type");
//		String subtype = req.getParameter("subtype");
		details.put("type", type);
		String sysId = null;
		if ("computer".equalsIgnoreCase(type)) {
//		if ("computer".equalsIgnoreCase(type) ||
//				("officeelectronics".equalsIgnoreCase(type) && "Printer".equalsIgnoreCase(subtype))) {
			JSONObject newCI = buildCIFromRequest(req, details);
			details.put("newData", newCI);
			String name = (String) newCI.get("name");
			String serviceNowType = (String) newCI.get("serviceNowType");
			JSONObject oldCI = fetchServiceNowCI(name, serviceNowType, details);
			details.put("existingData", oldCI);
			if (oldCI != null) {
				sysId = (String) ((JSONObject) oldCI.get("sys_id")).get("value");
				if (ciUnchanged(newCI, oldCI)) {
					response = "1;No action taken -- no changes detected";
				} else {
					response = updateServiceNowCI(newCI, sysId, details);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- CI not on file";
				} else {
					if (!"In Use".equalsIgnoreCase((String) newCI.get("hardware_substatus"))) {
						response = "1;No action taken -- INACTIVE CIs are not inserted";
					} else {
						response = insertServiceNowCI(newCI, details);
					}
				}
			}
		} else {
			response = "2;Records of type \"" + type + "\" are ignored.";
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow CI data on file, if present.</p>
	 *
	 * @param name the name of the ci
	 * @param serviceNowType either Personal Computer or Server
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow user data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowCI(String name, String serviceNowType, JSONObject details) {
		JSONObject ci = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow CI data for CI named \"" + name + "\" of type " + serviceNowType);
		}
		try {
			String url = serviceNowServer + FETCH_PC_URL + URLEncoder.encode(name, "UTF-8");
			if ("server".equalsIgnoreCase(serviceNowType)) {
				url = serviceNowServer + FETCH_SERVER_URL + URLEncoder.encode(name, "UTF-8");
			} else if ("printer".equalsIgnoreCase(serviceNowType)) {
				url = serviceNowServer + FETCH_PRINTER_URL + URLEncoder.encode(name, "UTF-8");
			}
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching ci data using url " + url);
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
				eventService.logEvent(new Event("ServletError", "CI fetch error", "Invalid HTTP Response Code returned when fetching user data for CI " + name + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user data for CI " + name + ": " + rc);
				}
			}
			JSONArray cis = (JSONArray) result.get("result");
			if (cis != null && cis.size() > 0) {
				ci = (JSONObject) cis.get(0);
				if (log.isDebugEnabled()) {
					log.debug("CI found for name " +name + ": " + ci);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("CI not found for name " + name);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for CI with name " + name + ": " + e, e);
			eventService.logEvent(new Event("ServletError", "User fetch exception", "Exception encountered searching for CI with name " + name + ": " + e, details, e));
		}

		return ci;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newCI the new data for this ci
	 * @param oldCI the existing data for this ci
	 * @return true if the ci is unchanged
	 */
	private boolean ciUnchanged(JSONObject newCI, JSONObject oldCI) {
		boolean unchanged = true;

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!isEqual(newCI, oldCI, field)) {
				unchanged = false;
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newCI the new data for this ci
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowCI(JSONObject newCI, JSONObject details) {
		String response = null;

		String serviceNowType = (String) newCI.get("serviceNowType");
		if (log.isDebugEnabled()) {
			log.debug("Inserting CI " + newCI.get("name") + " of type " + serviceNowType);
		}		
		
		// create HttpPost
		String url = serviceNowServer + UPDATE_PC_URL;
		if ("server".equalsIgnoreCase(serviceNowType)) {
			url = serviceNowServer + UPDATE_SERVER_URL;
		} else if ("printer".equalsIgnoreCase(serviceNowType)) {
			url = serviceNowServer + UPDATE_PRINTER_URL;
		}
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
			if (StringUtils.isNotEmpty((String) newCI.get(field))) {
				insertData.put(field, newCI.get(field));
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
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			JSONObject result = new JSONObject();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
					result = (JSONObject) jsonResponse.get("result");
					if (result != null) {
						String sysId = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty("sys_id")) {
							newCI.put("sys_id", sysId);
						}
					}
				}
			}
			if (rc == 200 || rc == 201) {
				response = "0;CI inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
				response = "2;Unable to insert CI";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new CI: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event("ServletError", "CI insert error", "Invalid HTTP Response Code returned when inserting new CI: " + rc, details));
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new CI " + newCI.get("name") + ": " + e, e);
			eventService.logEvent(new Event("ServletError", "CI insert exception", "Exception occured when attempting to insert new CI " + newCI.get("name") + ": " + e, details, e));
			response = "2;Unable to insert CI";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newCI the new data for this ci
	 * @param sysId the existing ci's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowCI(JSONObject newCI, String sysId, JSONObject details) {
		String response = null;

		String serviceNowType = (String) newCI.get("serviceNowType");
		if (log.isDebugEnabled()) {
			log.debug("Updating CI " + newCI.get("name") + " of type " + serviceNowType);
		}		
		
		// create HttpPut
		String url = serviceNowServer + UPDATE_PC_URL + "/" + sysId;
		if ("server".equalsIgnoreCase(serviceNowType)) {
			url = serviceNowServer + UPDATE_SERVER_URL + "/" + sysId;
		} else if ("printer".equalsIgnoreCase(serviceNowType)) {
			url = serviceNowServer + UPDATE_PRINTER_URL + "/" + sysId;
		}
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON to put
		JSONObject updateData = new JSONObject();
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if ("true".equalsIgnoreCase((String) newCI.get(field + "HasChanged"))) {
				String value = (String) newCI.get(field);
				if (StringUtils.isNotEmpty(value)) {
					updateData.put(field, value);
				} else {
					// send the mac address, even if it is blank, to correct malformed values
					if ("mac_address".equalsIgnoreCase(field)) {
						updateData.put(field, value);
					}
					
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
				response = "0;CI updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update CI";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating CI with sys_id " + sysId + ": " + rc);
				}
				eventService.logEvent(new Event("ServletError", "CI update error", "Invalid HTTP Response Code returned when updating CI with sys_id " + sysId + ": " + rc, details));
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
			log.debug("Exception occured when attempting to update CI " + newCI.get("name") + ": " + e);
			eventService.logEvent(new Event("ServletError", "CI update exception", "Exception occured when attempting to update CI " + newCI.get("name") + ": " + e, details, e));
			response = "2;Unable to update CI";
		}

		return response;
	}

	/**
	 * <p>Builds a new ci using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ci data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildCIFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject ci = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			ci.put(target, req.getParameter(source));
		}

		ci.put("name", fixName((String) ci.get("name")));
		ci.put("classification", determineClassification((String) ci.get("name")));
		ci.put("serviceNowType", determineServiceNowType(req.getParameter("subtype")));
		ci.put("mac_address", fixMacAddress((String) ci.get("mac_address")));
		if (StringUtils.isNotEmpty((String) ci.get("manufacturer"))) {
			ci.put("manufacturer", fixManufacturer((String) ci.get("manufacturer")));
		}
		ci.put("virtual", "false");
		if (((String) ci.get("name")).indexOf("VM") != -1) {
			ci.put("virtual", "true");
		}
		if (StringUtils.isNotEmpty((String) ci.get("hardware_status"))) {
			if (((String) ci.get("hardware_status")).toLowerCase().startsWith("in")) {
				ci.put("hardware_status", "Installed");
				ci.put("hardware_substatus", "In Use");
			} else {
				ci.put("hardware_status", "Retired");
				ci.put("hardware_substatus", "Reserved");
			}
		}

		for (int x=0; x<PERSON_PROPERTY.length; x++) {
			String field = PERSON_PROPERTY[x];
			if (StringUtils.isNotEmpty((String) ci.get(field))) {
				ci.put(field, getUserSysId((String) ci.get(field), details));
			}
		}

		i = referenceURL.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (StringUtils.isNotEmpty((String) ci.get(field))) {
				ci.put(field, getReferenceSysId(field, (String) ci.get(field), details));
			}
		}

		if (StringUtils.isNotEmpty((String) ci.get("department"))) {
			String department = (String) ci.get("department");
			if (department.indexOf("|") != -1) {
				String[] parts = department.split("|");
				ci.put("department", parts[0]);
				ci.put("cost_center", parts[1]);
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Returning new CI values: " + ci);
		}

		return ci;
	}

	private String fixName(String name) {
		if (StringUtils.isEmpty(name)) {
			name = "";
		}
		name = name.trim();
		if (name.startsWith("10.") || name.startsWith("152.") || name.indexOf(".") == -1) {
			// do not strip off suffix
		} else {
			name = name.substring(0, name.indexOf("."));
		}
		if (name.length() > 3 && name.indexOf("_") > 0) {
			name = name.substring(0, name.indexOf("_"));
		}
		return name.toUpperCase();
	}

	private String determineClassification(String name) {
		String classification = "Production";

        if (name.startsWith("DEV")) {
        	classification = "Development";
        } else if (name.startsWith("TEST")) {
        	classification = "Development Test";
        } else if (name.startsWith("TST")) {
        	classification = "Development Test";
        } else if (name.startsWith("STAGE")) {
        	classification = "UAT";
        } else if (name.startsWith("STG")) {
        	classification = "UAT";
        } else if (name.startsWith("DR")) {
        	classification = "Disaster Recovery";
        }
		
		return classification;
	}

	private String determineServiceNowType(String subtype) {
		String serviceNowType = "Personal Computer";

		if ("printer".equalsIgnoreCase(subtype)) {
			serviceNowType = "Printer";
		} else if ("unix".equalsIgnoreCase(subtype)) {
			serviceNowType = "Server";
		} else if ("windows".equalsIgnoreCase(subtype)) {
			serviceNowType = "Server";
		} else if ("server".equalsIgnoreCase(subtype)) {
			serviceNowType = "Server";
		}

		return serviceNowType;
	}

	private String fixMacAddress(String input) {
		String macAddress = "";

		if (StringUtils.isNotEmpty(input) && !"000000 00000000E0".equalsIgnoreCase(input)) {
			macAddress = input;
			macAddress = macAddress.trim();
			macAddress = macAddress.replace(" ", "");
			macAddress = macAddress.replace(":", "");
			macAddress = macAddress.replace("-", "");
			if (macAddress.length() == 12) {
				macAddress = macAddress.substring(0, 2) + ':' + macAddress.substring(2, 4) + ':' + macAddress.substring(4, 6) + ':' + macAddress.substring(6, 8) + ':' + macAddress.substring(8, 10) + ':' + macAddress.substring(10, 12);
			} else {
				macAddress = "";
			}
		}

		return macAddress.toUpperCase();
	}

	private String fixManufacturer(String manufacturer) {
		String originalManufacturer = manufacturer;

		manufacturer = manufacturer.trim().toLowerCase();
		if (manufacturer.indexOf("apple") != -1) {
			manufacturer = "Apple";
		} else if (manufacturer.indexOf("compaq") != -1) {
			manufacturer = "HP";
		} else if (manufacturer.indexOf("dell") != -1) {
			manufacturer = "Dell";
		} else if (manufacturer.indexOf("fujitsu") != -1) {
			manufacturer = "Fujitsu";
		} else if (manufacturer.indexOf("gateway") != -1) {
			manufacturer = "Gateway";
		} else if (manufacturer.indexOf("hewlett") != -1) {
			manufacturer = "HP";
		} else if (manufacturer.indexOf("ibm") != -1) {
			manufacturer = "IBM";
		} else if (manufacturer.indexOf("lenov") != -1) {
			manufacturer = "Lenovo";
		} else if (manufacturer.indexOf("microsoft") != -1) {
			manufacturer = "Microsoft";
		} else if (manufacturer.indexOf("panasonic") != -1) {
			manufacturer = "Panasonic";
		} else if (manufacturer.indexOf("sony") != -1) {
			manufacturer = "Sony";
		} else if (manufacturer.indexOf("surface") != -1) {
			manufacturer = "Microsoft";
		} else if (manufacturer.indexOf("toshiba") != -1) {
			manufacturer = "Toshiba";
		} else if (manufacturer.indexOf("vmware") != -1) {
			manufacturer = "VMware";
		} else if (manufacturer.indexOf("hp") != -1) {
			manufacturer = "HP";
		} else {
			manufacturer = originalManufacturer.trim();
		}

		return manufacturer;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the ci for whom the sys_id is being requested
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
	 * @param iamId the IAM ID for the ci for whom the sys_id is being requested
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
	 * @param userId the User ID for the user for whom the sys_id is being requested
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the iamId passed
	 */
	private String fetchUserSysId(String userId, JSONObject details) {
		String sysId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow sys_id for User ID " + userId);
		}

		try {
			String url = serviceNowServer + SYSID_URL + URLEncoder.encode(userId, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for User ID " + userId + ": " + rc);
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
							log.debug("sys_id found for User ID " + userId + ": " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for User ID " + userId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for User ID " + userId + ": " + e, e);
			eventService.logEvent(new Event("ServletError", "User sys_id fetch exception", "Exception encountered searching for sys_id for User ID " + userId + ": " + e, details, e));
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
					if ("department".equalsIgnoreCase(field)) {
						JSONObject cc = (JSONObject) obj.get("cost_center");
						if (cc != null) {
							sysId += "|" + cc.get("value");
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
			eventService.logEvent(new Event("ServletError", field + " fetch exception", "Exception encountered searching for sys_id for " + field + " " + value + ": " + e, details, e));
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
