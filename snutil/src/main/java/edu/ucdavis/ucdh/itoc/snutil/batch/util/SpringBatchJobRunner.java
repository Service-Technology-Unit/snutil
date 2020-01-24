package edu.ucdavis.ucdh.itoc.snutil.batch.util;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.ucdavis.ucdhs.isweb.core.utils.BatchJobService;
import edu.ucdavis.ucdhs.isweb.core.utils.BatchJobServiceStatistic;

/**
 * 
 * <p>Spring batch job runner.</p>
 *
 */
public class SpringBatchJobRunner {
	private static final Log log = LogFactory.getLog(SpringBatchJobRunner.class);

	/** 
	 * <p>The Spring batch job runner main method.</p>
	 *
	 * @param args the run-time arguments
	 */
	public static void main(String[] args) {
		// initiate batch job run
		log.info("SpringBatchJobRunner starting.");
		log.info(" ");

		// establish needed properties
		String appName = null;
		String jobName = null;
		String schlName = null;
		int batchJobInstanceId = 0;
		BatchJobService batchJobService = null;
		List<BatchJobServiceStatistic> stats = null;

		// validate bootstrap parameters
		if (args.length < 1) {
			log.error("Spring context file not provided - job terminated.");
			System.exit(1);
		}

		// load Spring context
		String springContextFile = args[0];
		log.info("Using Spring context file " + springContextFile);
		log.info(" ");
		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext(springContextFile);
		if (context.getBeanDefinitionNames().length == 0) {
			log.error("No Spring context available - job terminated.");
			System.exit(1);
		}  

		// load batch job schedule information
		try {
			appName = (String) context.getBean("appName");
			jobName = (String) context.getBean("jobName");;
			schlName = (String) context.getBean("schlName");
		} catch(Exception e) {
			// no one cares
		}
		if ( schlName != null ) {
			log.info("The batch job application name for this run is " + appName);
			log.info("The batch job job name for this run is " + jobName);
			log.info("The batch job schedule name for this run is " + schlName);
			log.info(" ");
			try {
				batchJobService = (BatchJobService) context.getBean("batchJobService");
			} catch(Exception e) {
				// no one cares
			}
			if (batchJobService == null) {
				log.error("Schedule ID provided, but no BatchJobService configured - job terminated.");
				System.exit(1);
			}
		}

		// load application to run
		SpringBatchJob springBatchJob = (SpringBatchJob) context.getBean("application");
		if (springBatchJob == null) {
			log.error("No application configured to run - job terminated.");
			System.exit(1);
		}

		// create & start batch job instance via monitoring API
		if (  schlName != null ) {
			try {
				batchJobInstanceId = batchJobService.createNewInstance(appName, jobName, schlName, "Instance created by SpringBatchJobRunner for " + springBatchJob.getClass().getSimpleName(), true);
				log.info("The batch job instance id for this run is " + batchJobInstanceId);
				log.info(" ");
			} catch (ExecutionException e) {
				log.warn("Unable to create batch job instance; job will continue, but status will not be reported", e);
				log.info(" ");
			}
		}

		// run application
		log.info("Launching application " + springBatchJob.getClass().getName());
		log.info(" ");
		try {
			stats = springBatchJob.run(args, batchJobInstanceId);
		} catch (Exception e) {
			String errorMessage = "Application " + springBatchJob.getClass().getSimpleName() + " terminated with an exception: " + e;
			log.error(errorMessage, e);
			if (batchJobInstanceId > 0) {
				try {
					batchJobService.updateInstance(batchJobInstanceId, BatchJobService.STATUS_FAILED, errorMessage);
				} catch (ExecutionException e1) {
					log.warn("Unable to report batch job failure", e);
				}
			}
			System.exit(1);
		}

		// report successful job completion via monitoring API
		if (batchJobInstanceId > 0) {
			try {
				batchJobService.updateInstance(batchJobInstanceId, BatchJobService.STATUS_COMPLETED, "Application " + springBatchJob.getClass().getSimpleName() + " completed successfully", stats);
			} catch (ExecutionException e) {
				log.info(" ");
				log.warn("Unable to report batch job completion", e);
			}
		}

		// normal batch job completion
		log.info(" ");
		log.info("SpringBatchJobRunner complete.");
		if (stats != null && stats.size() > 0) {
			log.info(" ");
			for (int i=0; i<stats.size(); i++) {
				log.info(formatStat(stats.get(i)));
			}
		}
	}

	/** 
	 * <p>Formats one <code>BatchJobServiceStatistic</code>.</p>
	 *
	 * @param stat the <code>BatchJobServiceStatistic</code> to format
	 * @return the formatted <code>BatchJobServiceStatistic</code>
	 */
	private static String formatStat(BatchJobServiceStatistic stat) {
		String string = stat.getLabel().trim();

		string += ": ";
		if (BatchJobService.FORMAT_CURRENCY.equalsIgnoreCase(stat.getFormat())) {
			string += "$" + formatNumber(stat.getValue().toString(), 2);
		} else if (BatchJobService.FORMAT_DATE.equalsIgnoreCase(stat.getFormat())) {
			string += formatDate(new Date(stat.getValue().longValue()), false);
		} else if (BatchJobService.FORMAT_DATE_TIME.equalsIgnoreCase(stat.getFormat())) {
			string += formatDate(new Date(stat.getValue().longValue()), true);
		} else if (BatchJobService.FORMAT_DURATION.equalsIgnoreCase(stat.getFormat())) {
			string += formatDuration(stat.getValue());
		} else if (BatchJobService.FORMAT_INTEGER.equalsIgnoreCase(stat.getFormat())) {
			string += formatNumber(stat.getValue().toString(), 0);
		} else if (BatchJobService.FORMAT_PERCENTAGE.equalsIgnoreCase(stat.getFormat())) {
			string += formatNumber(stat.getValue().toString(), 2) + "%";
		}

		return string;
	}

	/** 
	 * <p>Formats a date.</p>
	 *
	 * @param date the date to format
	 * @param includeTime when true, include the time
	 * @return the formatted date
	 */
	private static String formatDate(Date date, boolean includeTime) {
		String string = "";

		if (date != null) {
			string = date.toString();
		}

		return string;
	}

	/** 
	 * <p>Formats a duration.</p>
	 *
	 * @param duration the duration to format
	 * @return the formatted duration
	 */
	private static String formatDuration(BigInteger duration) {
		String string = "";

		if (duration != null) {
			string = formatNumber(duration.toString(), 3) + " seconds";
		}

		return string;
	}

	/** 
	 * <p>Formats a number.</p>
	 *
	 * @param string the numeric value to format
	 * @param places the number of decimal places to use
	 * @return the formatted number
	 */
	private static String formatNumber(String value, int places) {
		String string = "";

		if (StringUtils.isEmpty(value)) {
			value = "";
		}
		while (value.length() < places) {
			value = "0" + value;
		}
		if (places > 0) {
			string = "." + value.substring(value.length() - places);
			value = value.substring(0, value.length() - places);
		}
		String separator = "";
		while (value.length() > 2) {
			string = value.substring(value.length() - 3) + separator + string;
			value = value.substring(0, value.length() - 3);
			separator = ",";
		}
		if (value.length() > 0) {
			string = value + separator + string;
		}

		return string;
	}
}
