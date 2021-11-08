package io.coodoo.workhorse.persistence.redis.control;

/**
 * Set of keys used by the Redis persistence implementation
 * 
 * @author coodoo GmbH (coodoo.io)
 */
public enum RedisKey {

    /**
     * Store the configurations of the job engine
     * 
     * workhorse:config
     */
    CONFIG("w:c"),

    /**
     * Value to use as ID for Job.
     * 
     * workhorse:job:index
     */
    JOB_ID_INDEX("w:j:i"),

    /**
     * List of IDs of all jobs
     */
    JOB_LIST("w:j:l"),

    /**
     * Store the job with attributes as JSON
     * 
     * workhorse:job:<jobId>
     */
    JOB_BY_ID("w:j:%s"),

    /**
     * Store the ID of the job with the given workername
     * 
     * workhorse:job:workername:<workerClassName>
     */
    JOB_BY_WORKER_NAME("w:j:wn:%s"),

    /**
     * Store the ID of the job with the given name
     * 
     * workhorse:job:name:<jobName>
     */
    JOB_BY_NAME("w:j:n:%s"),

    /**
     * List of jobs by status
     * 
     * workhorse:job:status:<jobStatus>:list
     */
    JOB_BY_STATUS_LIST("w:j:s:%s:l"),

    // KEYS FOR EXECUTION

    /**
     * Value to use as ID for execution.
     * 
     * workhorse:execution:index
     */
    EXECUTION_ID_INDEX("w:j:e:i"),

    /**
     * Store the execution with all attributes as JSON
     * 
     * workhorse:job:<jobId>:execution:<executionId>
     */
    EXECUTION_BY_ID("w:j:%s:e:%s"),

    /**
     * Store the executionId associated with the given parameter hash
     * 
     * workhorse:job:<jobId>:execution:paramHash:<parametersHash>
     */
    EXECUTION_BY_PARAMETER_HASH("w:j:%s:e:p:%s"),

    /**
     * List of all executionId by JobId
     * 
     * workhorse:job:<jobId>:execution:list
     */
    EXECUTION_BY_JOB_LIST("w:j:%s:e:l"),

    /**
     * List of all executionId by status
     * 
     * workhorse:job:<jobId>:execution:executionStatus:<status>:list
     */
    EXECUTION_OF_JOB_BY_STATUS_LIST("w:j:%s:e:s:%s:l"),

    /**
     * List of all executionId by batchId
     * 
     * workhorse:job:<jobId>:execution:batch:<batchId>:list
     */
    EXECUTION_OF_BATCH_LIST("w:j:%s:e:b:%s:l"),

    /**
     * List of all executionId by chainId
     * 
     * workhorse:job:<jobId>:execution:chain:<batchId>:list
     */
    EXECUTION_OF_CHAIN_LIST("w:j:%s:e:c:%s:l"),

    // KEYS FOR EXECUTION LOG

    /**
     * Store the log execution with all attributes as JSON
     * 
     * workhorse:job:<jobId>:execution:<executionId>:log
     */
    EXECUTION_LOG_BY_ID("w:j:%s:e:%s:lg"),

    /**
     * Store the logs of an execution
     * 
     * workhorse:job:<jobId>:execution:<executionId>:log:list
     */
    EXECUTION_LOG_BY_ID_LIST("w:j:%s:e:%s:lg:l"),

    /**
     * Store the stacktrace of an execution
     * 
     * workhorse:job:<jobId>:execution:<executionId>:stacktrace:
     */
    EXECUTION_STACKTRACE_BY_ID("w:j:%s:e:%s:st"),

    // KEYS FOR WORKHORSE LOG

    /**
     * Value to use as Id for JobEngineLog.
     * 
     * workhorse:log:index
     */
    WORKHORSE_LOG_ID_INDEX("w:lg:i"),

    /**
     * Store a workhorseLog with all attributes as JSON
     * 
     * workhorse:log:<workhorseLogId>
     */
    WORKHORSE_LOG_BY_ID("w:lg:%s"),

    /**
     * List of workhorse logIds by jobId
     * 
     * workhorse:log:job:<jobId>:list
     */
    WORKHORSE_LOG_BY_JOB_LIST("w:j:%s:lg:l"),

    /**
     * List of all workhorse logs
     * 
     * workhorse:log:list
     */
    WORKHORSE_LOG_LIST("w:lg:l"),

    /**
     * Channel for queue
     */
    QUEUE_CHANNEL("w:j:%s:e");

    private String query;

    private RedisKey(String query) {
        this.query = query;
    }

    public String getQuery(Object... params) {
        if (params == null || params.length == 0) {
            return query;
        }
        return String.format(query, params);
    }
}
