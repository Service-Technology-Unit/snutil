package edu.ucdavis.ucdh.stu.snutil.util;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;

public class EventPoster extends Thread {
	private static final Random RANDOM = new Random();
	private Log log = LogFactory.getLog(getClass());
	private int instance = 0;
	private String url = null;
	private String user = null;
	private String password = null;
	private String source = null;
	private String sourceInstance = null;
	private String node = null;
	private Event event = null;
	private HttpClient client = null;

	EventPoster(int instance, String url, String user, String password, String source, String sourceInstance, String node, Event event, HttpClient client) {
		this.instance = instance;
		this.url = url;
		this.user = user;
		this.password = password;
		this.source = source;
		this.sourceInstance = sourceInstance;
		this.node = node;
		this.event = event;
		this.client = client;
	}

	@Override
	public void run() {
		int rc = 0;
		for (int attempt = 0; rc != 200 && rc != 201 && attempt < 25; ++attempt) {
			if (attempt > 0) {
				int pause = computePauseSeconds(attempt);
				if (log.isDebugEnabled()) {
					log.debug("EventPoster (#" + instance + ") sleeping for " + pause + " seconds.");
				}
				try {
					Thread.sleep(1000 * pause);
				}
				catch (InterruptedException e) {
					log.error("Sleep interrupted: " + e.getMessage(), (Throwable)e);
				}
			}
			rc = post();
		}
	}

	@SuppressWarnings("unchecked")
	private int post() {
		int rc = 0;

		// create HttpPost
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

		// build JSON object to post
		JSONObject eventData = new JSONObject();
		eventData.put("source", source);
		if (StringUtils.isNotEmpty(event.getSource())) {
			eventData.put("source", event.getSource());
		}
		eventData.put("event_class", sourceInstance);
		eventData.put("resource", event.getResource());
		eventData.put("node", node);
		eventData.put("metric_name", event.getMetricName());
		eventData.put("type", "Java Program");
		eventData.put("severity", event.getSeverity());
		eventData.put("description", event.getDescription());
		eventData.put("resolution_state", event.getResolutionState());
		if (event.getAdditionalInfo() != null) {
			eventData.put("additional_info", event.getAdditionalInfo().toJSONString());
		}

		// post JSON object
		try {
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(user, password), post, null));
			post.setEntity(new StringEntity(eventData.toJSONString()));
			if (log.isDebugEnabled()) {
				log.debug("EventPoster (#" + instance + ") posting JSON data to " + url);
				log.debug("EventPoster (#" + instance + ") posting JSON object: " + eventData.toJSONString());
			}
			HttpResponse resp = client.execute(post);
			rc = resp.getStatusLine().getStatusCode();
			if (rc != 200 && rc != 201) {
				log.warn("EventPoster (#" + instance + ") received invalid response code from post: " + rc);
			}
			String jsonRespString = "";
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				JSONObject jsonResponse = (JSONObject) JSONValue.parse(jsonRespString);
				if (jsonResponse != null) {
					jsonRespString = jsonResponse.toJSONString();
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("EventPoster (#" + instance + ") response from post: " + jsonRespString);
			}
		} catch (Exception e) {
			log.debug("Exception occurred from EventPoster (#" + instance + ") post attempt: " + e);
		}

		return rc;
	}

	private int computePauseSeconds(int attempt) {
		return 10 * attempt + RANDOM.nextInt(10);
	}
}

