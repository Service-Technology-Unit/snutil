package edu.ucdavis.ucdh.itoc.snutil.batch.util;

import java.util.List;

import edu.ucdavis.ucdhs.isweb.core.utils.BatchJobServiceStatistic;

/**
 * 
 * <p>Spring batch job interface.</p>
 *
 */
public interface SpringBatchJob {

	/**
	 * <p>This is the main "run" method called by the job runner.</p>
	 */
	public List<BatchJobServiceStatistic> run(String[] args, int jobId) throws Exception;
}
