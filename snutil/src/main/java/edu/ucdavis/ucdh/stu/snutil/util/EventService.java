package edu.ucdavis.ucdh.stu.snutil.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;

public class EventService {
	private static final String URI = "/api/now/table/em_event";
	private static int instance = 1;
	private Log log = null;
	private String source = null;
	private String sourceInstance = null;
	private String node = null;
	private String url = null;
	private String username = null;
	private String password = null;

	public EventService(String serviceEndPoint, String username, String password) {
		this.log = LogFactory.getLog(getClass());
		this.source = getClass().getName();
		this.sourceInstance = "1.0";
		this.node = "Unknown Host";
		try {
			this.node = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			log.error("Exception encountered attempting to obtain local host address: " + e, e);;
		}
		this.url = serviceEndPoint + URI;
		this.username = username;
		this.password = password;
	}

	/**
	 * <p>Used to log a single Event with the Event Management system</p>
	 *  
	 * @param the Event to log
	 */
	public void logEvent(Event event) {
		EventPoster eventPoster = new EventPoster(instance++, url, username, password, source, sourceInstance, node, event, createHttpClient());
		eventPoster.start();
	}

	/**
	 * <p>Builds and returns a new HttpClient.</p>
	 *  
	 * @return a new HttpClient
	 */
	private HttpClient createHttpClient() {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		try {
			SSLContext ctx = SSLContext.getInstance("TLSv1.2");
			X509TrustManager tm = new X509TrustManager(){

				public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}

				public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}

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
		}
		catch (Exception e) {
			log.error("Exception encountered: " + e.getClass().getName() + "; " + e.getMessage(), (Throwable)e);
		}
		return httpClient;
	}
}