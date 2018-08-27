package edu.ucdavis.ucdh.itoc.snutil.servlets;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.itoc.snutil.beans.Event;
import edu.ucdavis.ucdh.itoc.snutil.util.EventService;

/**
 * <p>This abstract class is the foundation for all ServiceNow Up2Date subscriber servlets.</p>
 */
public abstract class SubscriberServlet extends HttpServlet {
	private static final String COMPANY_URL = "/api/now/table/core_company?sysparm_fields=sys_id&sysparm_query=name%3DUC%20Davis%20Health";
	private static final long serialVersionUID = 1;
	protected Log log = LogFactory.getLog(getClass());
	protected EventService eventService = null;
	protected String serviceNowServer = null;
	protected String serviceNowUser = null;
	protected String serviceNowPassword = null;
	protected String ucDavisHealth = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		eventService = (EventService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("eventService");
		serviceNowServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowServer");
		serviceNowUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowUser");
		serviceNowPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowPassword");
		ucDavisHealth = fetchCompanySysId() ;
	}

	/**
	 * <p>The Servlet "GET" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The GET method is not allowed for this URL", null);
    }

	/**
	 * <p>The Servlet "doPost" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";
		String requestId = req.getParameter("_rid");
		String subscriptionId = req.getParameter("_sid");
		String publisherId = req.getParameter("_pid");
		String jobId = req.getParameter("_jid");
		String action = req.getParameter("_action");
		String id = req.getParameter("id");

		if (log.isDebugEnabled()) {
			log.debug("Processing new update - Publisher: " + publisherId + "; Subscription: " + subscriptionId + "; Request: " + requestId + "; Job: " + jobId + "; record: " + id);
		}

		JSONObject details = new JSONObject();
		details.put("requestId", requestId);
		details.put("subscriptionId", subscriptionId);
		details.put("publisherId", publisherId);
		details.put("jobId", jobId);
		details.put("action", action);
		details.put("id", id);

		if (StringUtils.isNotEmpty(action)) {
			if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete") || action.equalsIgnoreCase("force")) {
				response = processRequest(req, details);
			} else {
				response = "2;Error - Invalid action: " + action;
			}
		} else {
			response = "2;Error - Required parameter \"_action\" has no value";
		}

		if (log.isDebugEnabled()) {
			log.debug("Response: " + response);
		}

		res.setCharacterEncoding("UTF-8");
		res.setContentType("text/plain;charset=UTF-8");
		res.getWriter().write(response);
	}

	/**
	 * <p>Processes the incoming request.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the request processing
	 */
	protected abstract String processRequest(HttpServletRequest req, JSONObject details);

	/**
	 * <p>Returns the ServiceNow sys_id associated with the UC Davis Health company.</p>
	 *
	 * @return the ServiceNow sys_id associated with the UC Davis Health company
	 */
	private String fetchCompanySysId() {
		String sysId = null;

		try {
			String url = serviceNowServer + COMPANY_URL;
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow sys_id for UC Davis Health from URL " + url);
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
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow sys_id for UC Davis Health: " + rc);
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
							log.debug("sys_id found for UC Davis Health: " + sysId);
						}
					}
				}
			}
			if (log.isDebugEnabled()) {
				if (StringUtils.isEmpty(sysId)) {
					log.debug("sys_id not found for UC Davis Health");
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for sys_id for UC Davis Health: " + e, e);
			eventService.logEvent(new Event("", "Company fetch exception", "Exception encountered searching for sys_id for UC Davis Health: " + e, null, e));
		}

		return sysId;
	}

	/**
	 * <p>Builds and returns an HTTPClient.</p>
	 *
	 * @return an HTTPClient
	 */
	protected static HttpClient createHttpClient() {
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
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details) throws IOException {
		sendError(req, res, errorCode, errorMessage, details, null);
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 * @param throwable an optional exception
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details, Throwable throwable) throws IOException {
		// log message
		if (throwable != null) {
			log.error("Sending error " + errorCode + "; message=" + errorMessage, throwable);
		} else if (log.isDebugEnabled()) {
			log.debug("Sending error " + errorCode + "; message=" + errorMessage);
		}

		// log event
		eventService.logEvent(new Event((String) details.get("id"), "HTTP response", "Sending error " + errorCode + "; message=" + errorMessage, details, throwable));

		// send error
		res.setContentType("text/plain;charset=UTF-8");
		res.sendError(errorCode, errorMessage);
	}
}
