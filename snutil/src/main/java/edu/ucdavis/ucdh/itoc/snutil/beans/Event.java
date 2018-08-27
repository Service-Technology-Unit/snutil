package edu.ucdavis.ucdh.itoc.snutil.beans;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;

public class Event implements Serializable {
	private static final long serialVersionUID = 1;
	public static final String SEVERITY_CRITICAL = "Critical";
	public static final String SEVERITY_MAJOR = "Major";
	public static final String SEVERITY_MINOR = "Minor";
	public static final String SEVERITY_WARNING = "Warning";
	public static final String SEVERITY_INFO = "Info";
	public static final String SEVERITY_CLEAR = "Clear";
	public static final String STATE_READY = "Ready";
	public static final String STATE_PROCESSED = "Processed";
	public static final String STATE_IGNORED = "Ignored";
	public static final String STATE_ERROR = "Error";
	public static final String RESOLUTION_NEW = "New";
	public static final String RESOLUTION_CLOSING = "Closing";
	private String source = null;
	private String resource = null;
	private String metricName = null;
	private String messageKey = null;
	private String severity = null;
	private String description = null;
	private String resolutionState = null;
	private JSONObject additionalInfo = null;

	public Event(String metricName, String messageKey, String description) throws IllegalArgumentException {
		this(metricName, messageKey, description, null, null, null, null);
	}

	public Event(String metricName, String messageKey, String description, String severity, String resolutionState) throws IllegalArgumentException {
		this(metricName, messageKey, description, severity, resolutionState, null, null);
	}

	public Event(String metricName, String messageKey, String description, JSONObject additionalInfo) throws IllegalArgumentException {
		this(metricName, messageKey, description, null, null, additionalInfo, null);
	}

	public Event(String metricName, String messageKey, String description, Throwable throwable) throws IllegalArgumentException {
		this(metricName, messageKey, description, null, null, null, throwable);
	}

	public Event(String metricName, String messageKey, String description, JSONObject additionalInfo, Throwable throwable) throws IllegalArgumentException {
		this(metricName, messageKey, description, null, null, additionalInfo, throwable);
	}

	public Event(String metricName, String messageKey, String description, String severity, String resolutionState, JSONObject additionalInfo) throws IllegalArgumentException {
		this(metricName, messageKey, description, severity, resolutionState, additionalInfo, null);
	}

	public Event(String metricName, String messageKey, String description, String severity, String resolutionState, Throwable throwable) throws IllegalArgumentException {
		this(metricName, messageKey, description, severity, resolutionState, null, throwable);
	}

	@SuppressWarnings("unchecked")
	public Event(String resource, String messageKey, String description, String severity, String resolutionState, JSONObject additionalInfo, Throwable throwable) throws IllegalArgumentException {
		StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
		if (getClass().getName().equals(caller.getClassName())) {
			caller =  Thread.currentThread().getStackTrace()[3];
		}
		this.source = caller.getClassName();
		this.resource = resource;
		this.metricName = caller.getClassName() + "." + caller.getMethodName() + " (line #" + caller.getLineNumber() + ")";
		this.messageKey = messageKey;
		this.severity = severity;
		if (StringUtils.isEmpty(this.severity)) {
			this.severity = SEVERITY_MINOR;
		}
		this.description = description;
		this.resolutionState = resolutionState;
		if (StringUtils.isEmpty(this.resolutionState)) {
			this.resolutionState = RESOLUTION_NEW;
		}
		this.additionalInfo = additionalInfo;
		if (throwable != null) {
			JSONObject exception = jsonifyException(throwable);
			if (this.additionalInfo == null) {
				this.additionalInfo = new JSONObject();
			}
			additionalInfo.put("exception", exception);
		}
	}

	/**
	 * <p>Returns a <code>JSONObject</code> containing the details of the exception.</p>
	 *
	 * @param throwable the exception to JSONify
	 * @return a <code>JSONObject</code> containing the details of the exception
	 */
	@SuppressWarnings("unchecked")
	private JSONObject jsonifyException(Throwable throwable) {
		JSONObject exception = new JSONObject();

		exception.put("class", throwable.getClass().getName());
		exception.put("message", throwable.getMessage());
		exception.put("stackTrace", getStackTrace(throwable));
		if (throwable.getCause() != null) {
			exception.put("rootCause", jsonifyException(throwable.getCause()));
		}

		return exception;
	}

	/**
	 * <p>Returns a String representation of the StackTrace of the exception.</p>
	 *
	 * @param throwable the exception
	 * @return a String representation of the StackTrace of the exception
	 */
	public static String getStackTrace(Throwable throwable) {
		Writer result = new StringWriter();
		PrintWriter printWriter = new PrintWriter(result);
		throwable.printStackTrace(printWriter);
		return result.toString();
	}

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @return the resource
	 */
	public String getResource() {
		return resource;
	}

	/**
	 * @return the metricName
	 */
	public String getMetricName() {
		return metricName;
	}

	/**
	 * @return the messageKey
	 */
	public String getMessageKey() {
		return messageKey;
	}

	/**
	 * @return the severity
	 */
	public String getSeverity() {
		return severity;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @return the resolutionState
	 */
	public String getResolutionState() {
		return resolutionState;
	}

	/**
	 * @return the additionalInfo
	 */
	public JSONObject getAdditionalInfo() {
		return additionalInfo;
	}
}

