package edu.ucdavis.ucdh.stu.snutil.servlets;

import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;

/**
 * <p>This servlet updates the ServiceNow proc_po table with data from the Eclipsys PO table.</p>
 */
public class PurchaseOrderUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String LINE_SQL = "SELECT LINE, FULLDESC, QTY, UNIT, PRICE, ITEM, TOTREC, VENDID, DOCNOTE, TAXABLE FROM POLINES WHERE PO=? AND CHANGE=? ORDER BY LINE";
	private static final String BUYER_SQL = "SELECT IAM_ID FROM ECLIPSYS_BUYER_XREF WHERE ID=?";
	private static final String LOCATION_SQL = "SELECT FDC_ID FROM ECLIPSYS_LOC_XREF WHERE FDC_Name IS NOT NULL AND ID=?";
	private static final String VENDOR_SQL = "SELECT VEND, DOCNOTE FROM VENDOR WHERE VENDID=?";
	private static final String CONTRACT_SQL = "SELECT POCONTNU, FAC, VENDID, POCONTDES, CDATE, SDATE, EDATE, RSDATE, REDATE, TDATEF, TDATEL, CONSOURCE, CONSTATUS, NUMLINES, USERP, DOCNOTE, DOCLNOTE, VENDCONU, MFGDCONU, JITPROGID  FROM CONTRACT WHERE POCONTNU=?";
	private static final String ECLIPSYS_URL = "/api/now/table/u_source_target?sysparm_fields=sys_id&sysparm_query=u_name%3DEclipsys";
	private static final String FETCH_URL = "/api/now/table/proc_po?sysparm_display_value=all&sysparm_query=u_source_id%3D";
	private static final String FETCH_LINES_URL = "/api/now/table/proc_po_item?sysparm_display_value=all&sysparm_query=purchase_order%3D";
	private static final String SYSID_URL = "/api/now/table/sys_user?sysparm_fields=sys_id&sysparm_query=employee_number%3D";
	private static final String DEPT_URL = "/api/now/table/cmn_department?sysparm_fields=sys_id&sysparm_query=id%3D";
	private static final String LOC_URL = "/api/now/table/cmn_location?sysparm_fields=sys_id&sysparm_query=u_location_code%3D";
	private static final String VENDOR_URL = "/api/now/table/core_company?sysparm_fields=sys_id&sysparm_query=u_extrefid%3D";
	private static final String CONTRACT_URL = "/api/now/table/ast_contract?sysparm_fields=sys_id&sysparm_query=u_source_id%3D";
	private static final String STOCKROOM_URL = "/api/now/table/alm_stockroom?sysparm_fields=sys_id&sysparm_query=u_source_system.name%3DEclipsys%5Eu_source_id%3D";
	private static final String UPDATE_URL = "/api/now/table/proc_po";
	private static final String UPDATE_LINE_URL = "/api/now/table/proc_po_item";
	private static final String VENDOR_INSERT_URL = "/api/now/table/core_company";
	private static final String CONTRACT_INSERT_URL = "/api/now/table/ast_contract";
	private static final String IT_DEPT_URL = "/api/now/table/cmn_department?sysparm_query=nameSTARTSWITHIT%20%5EORnameSTARTSWITHMED%3AAcad%20%5EORnameSTARTSWITHMED%3AResearch&sysparm_fields=id";
	private static final String[] PROPERTY = {"assigned_to:buyerId","contract:contractId", "department:departmentId","description:details","due_by:requiredBy","location:locationId","po_date:entryDate","requested_by:userId","ship_to:shipTo","short_description:notes","status:status","total_cost:value","u_source_id:id","vendor:vendorId"};
	private static final String[] PO_LINE_FIELD = {"cost", "discount", "list_price", "ordered_quantity", "part_number", "received_quantity", "remaining_quantity", "short_description", "status", "total_cost", "total_list_price", "u_line_number", "u_notes", "u_taxable", "u_unit_of_measure", "vendor"};
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private Map<String,String> referenceURL = new HashMap<String,String>();
	private Map<String,Map<String,String>> referenceCache = new HashMap<String,Map<String,String>>();
	private List<String> itDepartments = new ArrayList<String>();
	private DataSource utilDataSource = null;
	private DataSource eclipsysDataSource = null;
	private String eclipsysSysId = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		utilDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("utilDataSource");
		eclipsysDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("eclipsysDataSource");
		for (int i=0;i<PROPERTY.length;i++) {
			String[] parts = PROPERTY[i].split(":");
			fieldMap.put(parts[0], parts[1]);
		}
		referenceCache.put("user", new HashMap<String,String>());
		referenceURL.put("department", DEPT_URL);
		referenceCache.put("department", new HashMap<String,String>());
		referenceURL.put("location", LOC_URL);
		referenceCache.put("location", new HashMap<String,String>());
		referenceURL.put("vendor", VENDOR_URL);
		referenceCache.put("vendor", new HashMap<String,String>());
		referenceURL.put("contract", CONTRACT_URL);
		referenceCache.put("contract", new HashMap<String,String>());
		referenceURL.put("stockroom", STOCKROOM_URL);
		referenceCache.put("stockroom", new HashMap<String,String>());
		referenceCache.put("buyerxref", new HashMap<String,String>());
		referenceCache.put("locationxref", new HashMap<String,String>());
		loadITDepartments();
		eclipsysSysId = fetchEclipsysSysId();
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
		String departmentId = fixDepartmentId(req.getParameter("departmentId"));
		details.put("departmentId", departmentId);
		String sysId = null;
		if (itDepartments.contains(departmentId)) {
			String version = req.getParameter("version");
			String currentVersion = req.getParameter("currentVersion");
			if (StringUtils.isNotEmpty(version) && version.equals(currentVersion)) {
				JSONObject newPO = buildPOFromRequest(req, details);
				details.put("newData", newPO);
				String id = (String) newPO.get("u_source_id");
				JSONObject oldPO = fetchServiceNowPO(id, details);
				details.put("existingData", oldPO);
				if (oldPO != null) {
					sysId = (String) ((JSONObject) oldPO.get("sys_id")).get("value");
					if (poUnchanged(newPO, oldPO)) {
						response = "1;No action taken -- no changes detected";
					} else {
						response = updateServiceNowPO(newPO, sysId, details);
					}
				} else {
					if (action.equalsIgnoreCase("delete")) {
						response = "1;No action taken -- PO not on file";
					} else {
						response = insertServiceNowPO(newPO, details);
					}
				}
			} else {
				response = "2;Purchase Order versions that are not the current version are ignored.";
				if (log.isDebugEnabled()) {
					log.debug("Purchase Order version " + version + " is not the current version (" + currentVersion + ") -- transaction ignored.");
				}
			}
		} else {
			response = "2;Purchase Orders for nonIT departments are ignored.";
			if (log.isDebugEnabled()) {
				log.debug("Department " + departmentId + " is not an IT Department -- transaction ignored.");
			}
		}

		return response;
	}

	/**
	 * <p>Returns the ServiceNow PO data on file, if present.</p>
	 *
	 * @param id the id of the po
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow po data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowPO(String id, JSONObject details) {
		JSONObject po = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow PO data for PO #" + id);
		}
		try {
			String url = serviceNowServer + FETCH_URL + URLEncoder.encode(id, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching PO data using url " + url);
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
				eventService.logEvent(new Event((String) details.get("id"), "PO fetch error", "Invalid HTTP Response Code returned when fetching PO #" + id + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching PO #" + id + ": " + rc);
				}
			}
			JSONArray pos = (JSONArray) result.get("result");
			if (pos != null && pos.size() > 0) {
				po = fetchServiceNowPoLines((JSONObject) pos.get(0), details);
				if (log.isDebugEnabled()) {
					log.debug("PO #" + id + " found: " + po);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("PO #" + id + " not found");
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for PO #" + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO fetch exception", "Exception encountered searching for PO #" + id + ": " + e, details, e));
		}

		return po;
	}

	/**
	 * <p>Returns the ServiceNow PO and PO Line data on file, if present.</p>
	 *
	 * @param po the ServiceNow Purchase Order
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow po data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowPoLines(JSONObject po, JSONObject details) {
		String sys_id = (String) ((JSONObject) po.get("sys_id")).get("value");;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow PO lines for PO with sys_id" + sys_id);
		}
		try {
			String url = serviceNowServer + FETCH_LINES_URL + URLEncoder.encode(sys_id, "UTF-8");
			HttpGet get = new HttpGet(url);
			get.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching PO lines using url " + url);
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
				eventService.logEvent(new Event((String) details.get("id"), "PO line fetch error", "Invalid HTTP Response Code returned when fetching PO with sys_id " + sys_id + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching lines for POwith sys_id " + sys_id + ": " + rc);
				}
			}
			JSONArray lines = (JSONArray) result.get("result");
			if (lines != null && lines.size() > 0) {
				int lineCt = lines.size();
				po.put("lineCt", lineCt + "");
				po.put("lines", lines);
				if (log.isDebugEnabled()) {
					log.debug(lineCt + " line(s) found for PO with sys_id " + sys_id);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("No lines found for PO with sys_id " + sys_id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for PO with sys_id " + sys_id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO Line fetch exception", "Exception encountered searching for PO with sys_id " + sys_id + ": " + e, details, e));
		}

		return po;
	}

	/**
	 * <p>Compares the incoming data with the data already on file.</p>
	 *
	 * @param newPO the new data for this po
	 * @param oldPO the existing data for this po
	 * @return true if the po is unchanged
	 */
	@SuppressWarnings("unchecked")
	private boolean poUnchanged(JSONObject newPO, JSONObject oldPO) {
		boolean unchanged = true;

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String field = i.next();
			if (!isEqual(newPO, oldPO, field)) {
				unchanged = false;
			}
		}
		JSONArray oldLines = (JSONArray) oldPO.get("lines");
		JSONArray newLines = (JSONArray) newPO.get("lines");
		int oldLineCt = 0;
		int newLineCt = 0;
		try {
			oldLineCt = Integer.parseInt((String) oldPO.get("lineCt"));
		} catch (Exception e) {
			// no one cares
		}
		try {
			newLineCt = Integer.parseInt((String) newPO.get("lineCt"));
		} catch (Exception e) {
			// no one cares
		}
		if (oldLineCt > 0 || newLineCt > 0) {
			for (int j=0; j<newLineCt; j++) {
				JSONObject thisNewLine = (JSONObject) newLines.get(j);
				String newLineNr = (String) thisNewLine.get("u_line_number");
				JSONObject thisOldLine = null;
				for (int k=0; k<oldLineCt; k++) {
					String oldLineNr = "";
					JSONObject oldLine = (JSONObject) oldLines.get(k);
					if (oldLine != null) {
						JSONObject oldLineNrObject = (JSONObject) oldLine.get("u_line_number");
						if (oldLineNrObject != null) {
							oldLineNr = (String) oldLineNrObject.get("value");
						}
					}
					if (newLineNr.equals(oldLineNr)) {
						thisOldLine = oldLine;
					}
				}
				if (thisOldLine != null) {
					thisNewLine.put("sys_id", ((JSONObject) thisOldLine.get("sys_id")).get("value"));
					thisOldLine.put("lineUsed", "true");
					for (int k=0; k<PO_LINE_FIELD.length; k++) {
						String field = PO_LINE_FIELD[k];
						if (!isEqual(thisNewLine, thisOldLine, field)) {
							unchanged = false;
							thisNewLine.put("lineChanged", "true");
						}
					}
				} else {
					thisNewLine.put("lineAdded", "true");
					unchanged = false;
				}
			}
			for (int j=0; j<oldLineCt; j++) {
				JSONObject thisOldLine = (JSONObject) oldLines.get(j);
				if (StringUtils.isEmpty((String) thisOldLine.get("lineUsed"))) {
					JSONObject thisNewLine = (JSONObject) JSONValue.parse(thisOldLine.toJSONString());
					thisNewLine.put("lineDeleted", "true");
					newLines.add(thisNewLine);
					unchanged = false;
				}
			}
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPO the new data for this po
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowPO(JSONObject newPO, JSONObject details) {
		String response = null;

		String id = (String) newPO.get("u_source_id");
		if (log.isDebugEnabled()) {
			log.debug("Inserting PO #" + id);
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
			if (StringUtils.isNotEmpty((String) newPO.get(field))) {
				insertData.put(field, newPO.get(field));
			}
		}
		insertData.put("u_source_system", eclipsysSysId);
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
							newPO.put("sys_id", sysId);
						}
					}
				}
			}
			if (rc == 200 || rc == 201) {
				response = "0;PO inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				if (StringUtils.isNotEmpty((String) newPO.get("sys_id")) && Integer.parseInt((String) newPO.get("lineCt")) > 0) {
					response += insertServiceNowPoLines(newPO, details);
				}
			} else {
				response = "2;Unable to insert PO";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new PO: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "PO insert error", "Invalid HTTP Response Code returned when inserting new PO: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to insert new PO " + newPO.get("name") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO insert exception", "Exception occured when attempting to insert new PO " + newPO.get("name") + ": " + e, details, e));
			response = "2;Unable to insert PO";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPO the new data for this po
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String insertServiceNowPoLines(JSONObject newPO, JSONObject details) {
		String response = "";

		String id = (String) newPO.get("u_source_id");
		String poSysId = (String) newPO.get("sys_id");
		JSONArray lines = (JSONArray) newPO.get("lines");
		if (log.isDebugEnabled()) {
			log.debug("Inserting " + lines.size() + " lines for PO #" + id);
		}

		int insertCt = 0;
		int failedCt = 0;
		Iterator<JSONObject> i = lines.iterator();
		while (i.hasNext()) {
			JSONObject thisLine = i.next();
			thisLine.put("purchase_order", poSysId);
			if (insertServiceNowPoLine(thisLine, details)) {
				insertCt++;
			} else {
				failedCt++;
			}
		}
		if (insertCt > 0) {
			response += "; " + insertCt + " PO line(s) inserted";
		}
		if (failedCt > 0) {
			response += "; " + failedCt + " PO line inserts failed";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPO the new data for this po
	 * @param sysId the existing po's ServiceNow sys_id
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowPO(JSONObject newPO, String sysId, JSONObject details) {
		String response = null;

		String id = (String) newPO.get("u_source_id");
		if (log.isDebugEnabled()) {
			log.debug("Updating PO #" + id);
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
			if ("true".equalsIgnoreCase((String) newPO.get(field + "HasChanged"))) {
				String value = (String) newPO.get(field);
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
				response = "0;PO updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
				if (Integer.parseInt((String) newPO.get("lineCt")) > 0) {
					response += updateServiceNowPoLines(newPO, details);
				}
			} else {
				response = "2;Unable to update PO";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating PO with sys_id " + sysId + ": " + rc);
				}
				eventService.logEvent(new Event((String) details.get("id"), "PO update error", "Invalid HTTP Response Code returned when updating PO with sys_id " + sysId + ": " + rc, details));
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
			log.error("Exception occured when attempting to update PO " + newPO.get("name") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO update exception", "Exception occured when attempting to update PO " + newPO.get("name") + ": " + e, details, e));
			response = "2;Unable to update PO";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPO the new data for this po
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String updateServiceNowPoLines(JSONObject newPO, JSONObject details) {
		String response = "";

		String id = (String) newPO.get("u_source_id");
		String poSysId = (String) newPO.get("sys_id");
		JSONArray lines = (JSONArray) newPO.get("lines");
		if (log.isDebugEnabled()) {
			log.debug("Updatting " + lines.size() + " lines for PO #" + id);
		}

		int insertCt = 0;
		int updateCt = 0;
		int deleteCt = 0;
		int unchangedCt = 0;
		int failedCt = 0;
		Iterator<JSONObject> i = lines.iterator();
		while (i.hasNext()) {
			JSONObject thisLine = i.next();
			if (StringUtils.isNotEmpty((String) thisLine.get("lineAdded"))) {
				thisLine.put("purchase_order", poSysId);
				if (insertServiceNowPoLine(thisLine, details)) {
					insertCt++;
				} else {
					failedCt++;
				}
			} else if (StringUtils.isNotEmpty((String) thisLine.get("lineChanged"))) {
				if (updateServiceNowPoLine(thisLine, details)) {
					updateCt++;
				} else {
					failedCt++;
				}
			} else if (StringUtils.isNotEmpty((String) thisLine.get("lineDeleted"))) {
				if (deleteServiceNowPoLine(thisLine, details)) {
					deleteCt++;
				} else {
					failedCt++;
				}
			} else {
				unchangedCt++;
			}
		}
		if (insertCt > 0) {
			response += "; " + insertCt + " PO line(s) inserted";
		}
		if (updateCt > 0) {
			response += "; " + updateCt + " PO line(s) updated";
		}
		if (deleteCt > 0) {
			response += "; " + deleteCt + " PO line(s) deleted";
		}
		if (unchangedCt > 0) {
			response += "; " + unchangedCt + " PO line(s) unchanged";
		}
		if (failedCt > 0) {
			response += "; " + failedCt + " PO line actions failed";
		}

		return response;
	}

	/**
	 * <p>Inserts one PO Line.</p>
	 *
	 * @param line the new data for this PO line
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return true if the insert was successful
	 */
	@SuppressWarnings("unchecked")
	private boolean insertServiceNowPoLine(JSONObject line, JSONObject details) {
		boolean success = false;

		String lineNr = (String) line.get("u_line_number");
		if (log.isDebugEnabled()) {
			log.debug("Inserting line #" + lineNr);
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_LINE_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("purchase_order", line.get("purchase_order"));
		for (int j=0; j<PO_LINE_FIELD.length; j++) {
			String field = PO_LINE_FIELD[j];
			if (StringUtils.isNotEmpty((String) line.get(field))) {
				insertData.put(field, line.get(field));
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
				}
			}
			if (rc == 200 || rc == 201) {
				success = true;
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new PO Line: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "PO Line insert error", "Invalid HTTP Response Code returned when inserting new PO Line: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to insert new PO Line: " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO Line insert exception", "Exception occured when attempting to insert new PO Line: " + e, details, e));
		}

		return success;
	}

	/**
	 * <p>Updates one PO Line.</p>
	 *
	 * @param line the new data for this PO line
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return true if the insert was successful
	 */
	@SuppressWarnings("unchecked")
	private boolean updateServiceNowPoLine(JSONObject line, JSONObject details) {
		boolean success = false;

		String lineNr = (String) line.get("u_line_number");
		String sysId = (String) line.get("sys_id");
		if (log.isDebugEnabled()) {
			log.debug("Updating line #" + lineNr);
		}

		// create HttpPost
		String url = serviceNowServer + UPDATE_LINE_URL + "/" + sysId;
		HttpPut put = new HttpPut(url);
		put.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to put
		JSONObject updateData = new JSONObject();
		for (int j=0; j<PO_LINE_FIELD.length; j++) {
			String field = PO_LINE_FIELD[j];
			if ("true".equalsIgnoreCase((String) line.get(field + "HasChanged"))) {
				String value = (String) line.get(field);
				if (StringUtils.isNotEmpty(value)) {
					updateData.put(field, value);
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("JSON object to PUT: " + updateData.toJSONString());
		}

		// put parameters
		try {
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON data to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			JSONObject result = new JSONObject();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
				}
			}
			if (rc == 200 || rc == 201) {
				success = true;
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating PO Line: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "PO Line update error", "Invalid HTTP Response Code returned when updating PO Line: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to update Line: " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO Line update exception", "Exception occured when attempting to update PO Line: " + e, details, e));
		}

		return success;
	}

	/**
	 * <p>Deletes one PO Line.</p>
	 *
	 * @param line the new data for this PO line
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return true if the insert was successful
	 */
	@SuppressWarnings("unchecked")
	private boolean deleteServiceNowPoLine(JSONObject line, JSONObject details) {
		boolean success = false;

		String lineNr = (String) ((JSONObject) line.get("u_line_number")).get("value");
		String sysId = (String) ((JSONObject) line.get("sys_id")).get("value");
		if (log.isDebugEnabled()) {
			log.debug("Deleting line #" + lineNr);
		}

		// create HttpDelete
		String url = serviceNowServer + UPDATE_LINE_URL + "/" + sysId;
		HttpDelete delete = new HttpDelete(url);
		delete.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		delete.setHeader(HttpHeaders.ACCEPT, "application/json");
		delete.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// delete
		try {
			HttpClient client = createHttpClient();
			if (log.isDebugEnabled()) {
				log.debug("Deleting PO Line using URL " + url);
			}
			HttpResponse resp = client.execute(delete);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			JSONObject result = new JSONObject();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
				}
			}
			if (rc == 200 || rc == 202) {
				success = true;
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from delete: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when deleting PO Line: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "PO Line delete error", "Invalid HTTP Response Code returned when deleting PO Line: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to delete PO Line: " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO Line delete exception", "Exception occured when attempting to delete PO Line: " + e, details, e));
		}

		return success;
	}

	/**
	 * <p>Builds a new po using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the po data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildPOFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject po = new JSONObject();

		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String target = i.next();
			String source = fieldMap.get(target);
			po.put(target, req.getParameter(source));
		}
		po.put("lineCt", req.getParameter("lineCt"));
		if (StringUtils.isEmpty((String) po.get("short_description"))) {
			String title = "(no title)";
			String description = (String) po.get("description");
			if (StringUtils.isNotEmpty(description)) {
				String[] lines = description.split("\n");
				for (int x=0; x<lines.length; x++) {
					if (StringUtils.isNotEmpty(lines[x])) {
						title = lines[x];
						x = lines.length;
					}
				}
			}
			po.put("short_description", title);
		}

		po.put("assigned_to", translateBuyer((String) po.get("assigned_to"), details));
		po.put("department", translateDepartment((String) po.get("department"), details));
		po.put("location", translateLocation(req.getParameter("location"), details));
		po.put("requested_by", translateRequestedBy((String) po.get("requested_by"), details));
		po.put("status", translateStatus((String) po.get("status"), details));
		po.put("vendor", translateVendor((String) po.get("vendor"), details));
		po.put("contract", translateContract((String) po.get("contract"), details));
		po.put("ship_to", translateShipTo((String) po.get("ship_to"), details));
		po.put("lines", fetchPoLines((String) po.get("u_source_id"), req.getParameter("currentVersion"), details));

		if (log.isDebugEnabled()) {
			log.debug("Returning new PO values: " + po);
		}

		return po;
	}

	/**
	 * <p>Fetches the PO lines from the Eclipsys database.</p>
	 *
	 * @param id the Eclipsys Purchase Order ID
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return a JSONArray of the PO lines
	 */
	@SuppressWarnings("unchecked")
	private JSONArray fetchPoLines(String id, String version, JSONObject details) {
		JSONArray lines = new JSONArray();

		if (log.isDebugEnabled()) {
			log.debug("Fetching the PO lines for PO #" + id);
		}

		// fetch PO lines from Eclipsys
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = eclipsysDataSource.getConnection();
			ps = conn.prepareStatement(LINE_SQL);
			ps.setString(1, id);
			ps.setString(2, version);
			rs = ps.executeQuery();
			while (rs.next()) {
				JSONObject thisLine = new JSONObject();
				thisLine.put("status", "Ordered");
				thisLine.put("discount", "0");
				thisLine.put("cost", rs.getString("PRICE"));
				thisLine.put("list_price", rs.getString("PRICE"));
				thisLine.put("ordered_quantity", rs.getString("QTY"));
				thisLine.put("part_number", rs.getString("ITEM"));
				thisLine.put("received_quantity", rs.getString("TOTREC"));
				thisLine.put("short_description", rs.getString("FULLDESC"));
				thisLine.put("vendor", translateVendor(rs.getString("VENDID"), details));
				thisLine.put("u_line_number", rs.getString("LINE"));
				thisLine.put("u_notes", rs.getString("DOCNOTE"));
				thisLine.put("u_unit_of_measure", rs.getString("UNIT"));
				if ("Y".equalsIgnoreCase(rs.getString("TAXABLE"))) {
					thisLine.put("u_taxable", "true");
				} else {
					thisLine.put("u_taxable", "false");
				}
				float ordered = 0;
				float received = 0;
				try {
					ordered = Float.parseFloat(rs.getString("QTY"));
				} catch (Exception e) {
					// no one cares
				}
				try {
					received = Float.parseFloat(rs.getString("TOTREC"));
				} catch (Exception e) {
					// no one cares
				}
				if (received == ordered) {
					thisLine.put("status", "Received");
					thisLine.put("remaining_quantity", "0");
				} else {
					thisLine.put("remaining_quantity", (ordered - received) + "");
				}
				float total = 0;
				try {
					total = ordered * Float.parseFloat(rs.getString("PRICE"));
				} catch (Exception e) {
					// no one cares
				}
				thisLine.put("total_cost", total + "");
				thisLine.put("total_list_price", total + "");
				lines.add(thisLine);
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to fetch lines for PO #" + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO line fetch exception", "Exception occured when attempting to fetch lines for PO #" + id + ": " + e, details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					// no one cares
				}
			}
		}

		return lines;
	}

	private String translateBuyer(String originalValue, JSONObject details) {
		String buyerSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			String iamId = getBuyerId(originalValue, details);
			if (StringUtils.isNotEmpty(iamId)) {
				buyerSysId = getUserSysId(iamId, details);
			}
			if (StringUtils.isEmpty(buyerSysId)) {
				log.warn("Unable to translate Eclipsys Buyer #" + originalValue);
				eventService.logEvent(new Event((String) details.get("id"), "Eclipsys Buyer Translation Error", "Unable to translate Eclipsys Buyer #" + originalValue, details));
			}
		}

		return buyerSysId;
	}

	private String translateDepartment(String originalValue, JSONObject details) {
		String departmentSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			departmentSysId = getReferenceSysId("department", fixDepartmentId(originalValue), details);
		}

		return departmentSysId;
	}

	private String translateLocation(String originalValue, JSONObject details) {
		String locationSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			String locationId = getLocationId(originalValue, details);
			if (StringUtils.isNotEmpty(locationId)) {
				locationSysId = getReferenceSysId("location", locationId, details);
			}
			if (StringUtils.isEmpty(locationSysId)) {
				log.warn("Unable to translate Eclipsys Location " + originalValue);
				eventService.logEvent(new Event((String) details.get("id"), "Eclipsys Location Translation Error", "Unable to translate Eclipsys Locaation " + originalValue, details));
			}
		}

		return locationSysId;
	}

	private String translateRequestedBy(String originalValue, JSONObject details) {
		return "";
	}

	private String translateStatus(String originalValue, JSONObject details) {
		String status = "Ordered";

		int originalStatus = 0;
		try {
			originalStatus = Integer.parseInt(originalValue);
		} catch (Exception e) {
			// no one cares
		}
		if (originalStatus == 0) {
			status = "Canceled";
		} else if (originalStatus == 1) {
			status = "Requested";
		} else if (originalStatus == 6 || originalStatus == 9) {
			status = "Received";
		}

		return status;
	}

	private String translateVendor(String originalValue, JSONObject details) {
		String vendorSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			vendorSysId = getReferenceSysId("vendor", originalValue, details);
			if (StringUtils.isEmpty(vendorSysId)) {
				vendorSysId = addVendor(originalValue, details);
			}
		}

		return vendorSysId;
	}

	private String translateContract(String originalValue, JSONObject details) {
		String contractSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			contractSysId = getReferenceSysId("contract", originalValue, details);
			if (StringUtils.isEmpty(contractSysId)) {
				contractSysId = addContract(originalValue, details);
			}
		}

		return contractSysId;
	}

	private String translateShipTo(String originalValue, JSONObject details) {
		String shipToSysId = null;

		if (StringUtils.isNotEmpty(originalValue)) {
			shipToSysId = getReferenceSysId("stockroom", originalValue, details);
		}

		return shipToSysId;
	}

	/**
	 * <p>Adds the Vendor to the ServiceNow Company table.</p>
	 *
	 * @param vendorId the Eclipsys ID of the new vendor
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id of the added vendor
	 */
	@SuppressWarnings("unchecked")
	private String addVendor(String vendorId, JSONObject details) {
		String sys_id = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching Eclipsys vendor data for Vendor #" + vendorId);
		}

		// fetch vendor data from Eclipsys
		JSONObject vendor = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = eclipsysDataSource.getConnection();
			ps = conn.prepareStatement(VENDOR_SQL);
			ps.setString(1, vendorId);
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Eclipsys vendor data found for Vendor #" + vendorId);
				}
				vendor = new JSONObject();
				vendor.put("u_source_system", eclipsysSysId);
				vendor.put("u_source_id", vendorId);
				vendor.put("u_extrefid", vendorId);
				vendor.put("name", rs.getString("VEND"));
				String notes = "Vendor #" + vendorId + " inserted via integration during Purchase Order import.";
				if (StringUtils.isNotEmpty(rs.getString("DOCNOTE"))) {
					notes += "\n\n" + rs.getString("DOCNOTE");
				}
				vendor.put("notes", notes);
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to fetch Vendor " + vendorId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Vendor fetch exception", "Exception occured when attempting to fetch vendor " + vendorId + ": " + e, details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					// no one cares
				}
			}
		}

		if (vendor != null) {
			sys_id = insertVendor(vendor, details);
		}

		return sys_id;
	}

	/**
	 * <p>Adds the Vendor to the ServiceNow Company table.</p>
	 *
	 * @param vendor the Eclipsys vendor data
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id of the added vendor
	 */
	@SuppressWarnings("unchecked")
	private String insertVendor(JSONObject vendor, JSONObject details) {
		String sys_id = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting new Vendor #" + vendor.get("u_extrefid"));
		}

		// create HttpPost
		String url = serviceNowServer + VENDOR_INSERT_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		vendor.put("company", ucDavisHealth);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + vendor.toJSONString());
		}

		// post parameters
		try {
			post.setEntity(new StringEntity(vendor.toJSONString()));
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
						sys_id = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty(sys_id)) {
							vendor.put("sys_id", sys_id);
						}
					}
				}
			}
			if (rc == 200 || rc == 201) {
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
					if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new Vendor: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "Vendor insert error", "Invalid HTTP Response Code returned when inserting new Vendor: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to insert new vendor " + vendor.get("name") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Vendor insert exception", "Exception occured when attempting to insert new Vendor " + vendor.get("name") + ": " + e, details, e));
		}

		return sys_id;
	}

	/**
	 * <p>Adds the Contract to the ServiceNow Contract table.</p>
	 *
	 * @param contractId the Eclipsys ID of the new contract
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id of the added contract
	 */
	@SuppressWarnings("unchecked")
	private String addContract(String contractId, JSONObject details) {
		String sys_id = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching Eclipsys contract data for Contract #" + contractId);
		}

		// fetch contract data from Eclipsys
		JSONObject contract = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = eclipsysDataSource.getConnection();
			ps = conn.prepareStatement(CONTRACT_SQL);
			ps.setString(1, contractId);
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Eclipsys contract data found for Contract #" + contractId);
				}
				contract = new JSONObject();
				contract.put("u_source_system", eclipsysSysId);
				contract.put("u_source_id", contractId);
				contract.put("vendor", translateVendor(rs.getString("VENDID"), details));
				contract.put("short_description", rs.getString("POCONTDES"));
				contract.put("starts", rs.getString("SDATE"));
				contract.put("ends", rs.getString("EDATE"));
				contract.put("renewal_date", rs.getString("RSDATE"));
				contract.put("renewal_end_date", rs.getString("REDATE"));
//				contract.put("active", rs.getString("CONSTATUS"));
//				contract.put("contract_administrator", rs.getString("USERP"));
				contract.put("description", rs.getString("DOCLNOTE"));
				String notes = "Contract #" + contractId + " inserted via integration during Purchase Order import.";
				if (StringUtils.isNotEmpty(rs.getString("DOCNOTE"))) {
					notes += "\n\n" + rs.getString("DOCNOTE");
				}
				contract.put("terms_and_conditions", notes);
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to fetch Contract " + contractId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Contract fetch exception", "Exception occured when attempting to fetch contract " + contractId + ": " + e, details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					// no one cares
				}
			}
		}

		if (contract != null) {
			sys_id = insertContract(contract, details);
		}

		return sys_id;
	}

	/**
	 * <p>Adds the Contract to the ServiceNow Company table.</p>
	 *
	 * @param contract the Eclipsys contract data
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id of the added contract
	 */
	@SuppressWarnings("unchecked")
	private String insertContract(JSONObject contract, JSONObject details) {
		String sys_id = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting new Contract #" + contract.get("u_source_id"));
		}

		// create HttpPost
		String url = serviceNowServer + CONTRACT_INSERT_URL;
		HttpPost post = new HttpPost(url);
		post.addHeader(BasicScheme.authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), "UTF-8", false));
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		contract.put("company", ucDavisHealth);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + contract.toJSONString());
		}

		// post parameters
		try {
			post.setEntity(new StringEntity(contract.toJSONString()));
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
						sys_id = (String) result.get("sys_id");
						if (StringUtils.isNotEmpty(sys_id)) {
							contract.put("sys_id", sys_id);
						}
					}
				}
			}
			if (rc == 200 || rc == 201) {
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
			} else {
					if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new Contract: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "Contract insert error", "Invalid HTTP Response Code returned when inserting new Contract: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to insert new contract " + contract.get("name") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Contract insert exception", "Exception occured when attempting to insert new Contract " + contract.get("name") + ": " + e, details, e));
		}

		return sys_id;
	}

	/**
	 * <p>Returns the IAM ID associated with the Eclipsys buyer passed.</p>
	 *
	 * @param value the Buyer ID from the Eclipsys system
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the IAM ID associated with the Eclipsys buyer passed
	 */
	private String getBuyerId(String value, JSONObject details) {
		String returnValue = null;

		Map<String,String> cache = referenceCache.get("buyerxref");
		if (cache.containsKey(value)) {
			returnValue = cache.get(value);
		} else {
			returnValue = fetchBuyerId(value, details);
			if (StringUtils.isNotEmpty(returnValue)) {
				cache.put(value, returnValue);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning IAM ID \"" + returnValue + "\" for Eclipsys Buyer " + value);
		}

		return returnValue;
	}

	/**
	 * <p>Returns the IAM ID associated with the Eclipsys buyer passed.</p>
	 *
	 * @param value the Buyer ID from the Eclipsys system
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the IAM ID associated with the Eclipsys buyer passed
	 */
	private String fetchBuyerId(String id, JSONObject details) {
		String buyerId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching IAM ID for Eclipsys Buyer " + id);
		}

		// fetch  ID from buyer xref database
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = utilDataSource.getConnection();
			ps = conn.prepareStatement(BUYER_SQL);
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				buyerId = rs.getString("IAM_ID");
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to fetch IAM ID for Eclipsys Buyer " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Buyer IAM ID fetch exception", "Exception occured when attempting to fetch IAM ID for Eclipsys Buyer " + id + ": " + e, details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					// no one cares
				}
			}
		}

		return buyerId;
	}

	/**
	 * <p>Returns the FD&C Location ID associated with the Eclipsys value passed.</p>
	 *
	 * @param value the location ID from the Eclipsys system
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the FD&C Location ID associated with the Eclipsys value passed
	 */
	private String getLocationId(String value, JSONObject details) {
		String returnValue = null;

		Map<String,String> cache = referenceCache.get("locationxref");
		if (cache.containsKey(value)) {
			returnValue = cache.get(value);
		} else {
			returnValue = fetchLocationId(value, details);
			if (StringUtils.isNotEmpty(returnValue)) {
				cache.put(value, returnValue);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning FD&C Location ID " + returnValue + " for Eclipsys Location " + value);
		}

		return returnValue;
	}

	/**
	 * <p>Returns the FD&C Location ID associated with the Eclipsys value passed.</p>
	 *
	 * @param value the location ID from the Eclipsys system
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the FD&C Location ID associated with the Eclipsys value passed
	 */
	private String fetchLocationId(String id, JSONObject details) {
		String locationId = "";

		if (log.isDebugEnabled()) {
			log.debug("Fetching FD&C Location ID for Eclipsys Location " + id);
		}

		// fetch location ID from xref database
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = utilDataSource.getConnection();
			ps = conn.prepareStatement(LOCATION_SQL);
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				locationId = rs.getString("FDC_ID");
			}
		} catch (Exception e) {
			log.error("Exception occured when attempting to fetch FD&C Location ID for Eclipsys Location ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "PO line fetch exception", "Exception occured when attempting to fetch FD&C Location ID for Eclipsys Location ID " + id + ": " + e, details, e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					// no one cares
				}
			}
		}

		return locationId;
	}

	/**
	 * <p>Returns the ServiceNow sys_id associated with the iamId passed.</p>
	 *
	 * @param iamId the IAM ID for the po for whom the sys_id is being requested
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
	 * <p>Returns the ServiceNow sys_id associated with the information passed.</p>
	 *
	 * @param field the field for which the sys_id is being requested
	 * @param value the field value for which the sys_id is being requested
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow sys_id associated with the information passed
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
			eventService.logEvent(new Event((String) details.get("id"), "User sys_id fetch exception", "Exception encountered searching for sys_id for User ID " + userId + ": " + e, details, e));
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
			eventService.logEvent(new Event((String) details.get("id"), field + " fetch exception", "Exception encountered searching for sys_id for " + field + " " + value + ": " + e, details, e));
		}

		return sysId;
	}

	/**
	 * <p>Loads the list of Department IDs for all IT departments.</p>
	 *
	 */
	private void loadITDepartments() {
		try {
			String url = serviceNowServer + IT_DEPT_URL;
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_ids for all IT departments from URL " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_ids for all IT departments: " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				JSONArray result = (JSONArray) json.get("result");
				if (result != null && result.size() > 0) {
					for (int i=0; i<result.size(); i++) {
						JSONObject obj = (JSONObject) result.get(i);
						String id = (String) obj.get("id");
						if (StringUtils.isNotEmpty(id)) {
							itDepartments.add(id);
							if (log.isDebugEnabled()) {
								log.debug("Adding IT department: " + id);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for ids for all IT departments: " + e, e);
			eventService.logEvent(new Event("", "id fetch exception", "Exception encountered searching for ids for all IT departments: " + e, null, e));
		}
	}


	private String fixDepartmentId(String id) {
		if (StringUtils.isNotEmpty(id)) {
			id = id.replace("H", "H ");
		}
		return id;
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

	/**
	 * <p>Returns the ServiceNow sys_id associated with the Eclipsys Source/Target System.</p>
	 *
	 * @return the ServiceNow sys_id associated with the Eclipsys Source/Target System
	 */
	private String fetchEclipsysSysId() {
		String sysId = null;

		try {
			String url = serviceNowServer + ECLIPSYS_URL;
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id for Eclipsys source system from URL " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for Eclipsys: " + rc);
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
							log.debug("sys_id found for Eclipsys: " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for Eclipsys");
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for Eclipsys: " + e, e);
			eventService.logEvent(new Event("", "Source/Target System fetch exception", "Exception encountered searching for sys_id for Eclipsys: " + e, null, e));
		}

		return sysId;
	}
}
