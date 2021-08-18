package io.coodoo.workhorse.persistence.redis.control;

import java.util.Arrays;

import org.jboss.logging.Logger;

/**
 * Set of keys used by the redis persistence implementation
 * 
 * @author coodoo GmbH (coodoo.io)
 */
public enum RedisKey {

    /**
     * Store the configurations of the job engine
     */
    JOB_ENGINE_CONFIG("job:config"),

    /**
     * Value to use as ID for Job.
     */
    INC_JOB_ID("jobIndex"),

    /**
     * Store the job with attributes as JSON
     * 
     * job:<jobId>
     */
    JOB_BY_ID("job:%s"),

    /**
     * Store the ID of the job with the given workername
     * 
     * job:workername:<workerClassName>
     */
    JOB_BY_WORKER_NAME("job:workername:%s"),

    /**
     * Store the ID of the job with the given name
     * 
     * job:name:<jobName>
     */
    JOB_BY_NAME("job:name:%s"),

    /**
     * List of IDs of all jobs
     */
    LIST_OF_JOB("list:job"),

    /**
     * List of jobs by status
     * 
     * list:job:<JobStatus>
     */
    LIST_OF_JOB_BY_STATUS("list:job:%s"),

    // KEYS FOR EXECUTION

    /**
     * Value to use as ID for execution.
     */
    INC_EXECUTION_ID("executionIndex"),

    /**
     * Store the execution with all attributes as JSON
     * 
     * exe:<executionId>
     */
    EXECUTION_BY_ID("exe:%s"),

    /**
     * Store the executionId associated with the given parameterhash paramHash:<parametersHash>:exe
     */
    EXECUTION_BY_PARAMETER_HASH("paramHash:%s:exe"),

    /**
     * List of all executionId by JobId
     * 
     * list:job:<jobId>:exe
     */
    LIST_OF_EXECUTION_BY_JOB("list:job:%s:exe"),

    /**
     * Queue to store new created executions
     */
    EXECUTION_QUEUE("queue:exe"),

    /**
     * Queue to store new created executions
     */
    PRIORITY_EXECUTION_QUEUE("queue:exe:priority"),

    /**
     * List of all executionId by status
     * 
     * list:job:<jobId>:executionStatus:<status>:exe
     */
    LIST_OF_EXECUTION_OF_JOB_BY_STATUS("list:job:%s:executionStatus:%s:exe"),

    /**
     * List of all executionId by batchId
     * 
     * list:batch:<batchId>:exe
     */
    LIST_OF_EXECUTION_OF_BATCH("list:batch:%s:exe"),

    /**
     * List of all executionId by chainId
     * 
     * list:chain:<batchId>:exe
     */
    LIST_OF_EXECUTION_OF_CHAIN("list:chain:%s:exe"),

    // KEYS FOR EXECUTION LOG

    /**
     * Store the log execution with all attributes as JSON
     * 
     * exe:<executionId>
     */
    EXECUTION_LOG_BY_ID("log:%s:exe"),

    /**
     * Store the logs of an execution
     * 
     * log:<executionId>:exe
     */
    LIST_OF_EXECUTION_LOG_BY_ID("log:%s:exe"),

    /**
     * Store the error of an execution
     * 
     * error:<executionId>:exe
     */
    EXECUTION_ERROR_AND_STACKTRACE_BY_ID("error:%s:exe"),

    // KEYS FOR WORKHORSE_LOG

    /**
     * Value to use as Id for JobEngineLog.
     */
    INC_WORKHORSE_LOG_ID("workhorseLogIndex"),

    /**
     * Store a workhorseLog with all attributes as JSON
     * 
     * workhorseLog:<workhorseLogId>
     */
    WORKHORSE_LOG_BY_ID("workhorseLog:%s"),

    /**
     * List of workhorse logs by jobId
     * 
     * list:job:<jobId>:workhorselog
     */
    LIST_OF_WORKHORSE_LOG_BY_JOB("list:job:%s:workhorselog"),

    /**
     * List of all workhorse logs
     */
    WORKHORSE_LOG_LIST("list:workhorselog");

    private static Logger log = Logger.getLogger(RedisKey.class);

    String query;

    private RedisKey(String query) {
        this.query = query;
    }

    public String getQuery(Object... params) {
        if (params == null || params.length == 0) {
            return query;
        }

        if (countMatches(this.query, "%s") != params.length) {

            String message = "The number of parameters passed does not match the number of placeholders in the query! " + this.name() + " Query: " + this.query
                            + " -> parameter: " + Arrays.toString(params);
            log.error(message);
        }
        return String.format(query, params);
    }

    public static int countMatches(String str, String sub) {
        if (isEmpty(str) || isEmpty(sub)) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
}
