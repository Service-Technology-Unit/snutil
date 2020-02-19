package edu.ucdavis.ucdh.stu.snutil.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
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
	private HttpClient client = null;

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
		try {
			this.client = HttpClientProvider.getClient();
		} catch (Exception e) {
			log.error("Exception encountered attempting to build HTTP client: " + e, e);;
		}
	}

	/**
	 * <p>Used to log a single Event with the Event Management system</p>
	 *  
	 * @param the Event to log
	 */
	public void logEvent(Event event) {
		EventPoster eventPoster = new EventPoster(instance++, url, username, password, source, sourceInstance, node, event, client);
		eventPoster.start();
	}
}