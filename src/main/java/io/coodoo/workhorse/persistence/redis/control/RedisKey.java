package io.coodoo.workhorse.persistence.redis.control;

/**
 * Set of keys used by the redis persistence implementation
 * 
 * @author coodoo GmbH (coodoo.io)
 */
public enum RedisKey {

    /**
     * Store the configurations of the job engine
     * 
     * workhorse:config
     */
    WORKHORSE_CONFIG("w:c"),

    /**
     * Value to use as ID for Job.
     * 
     * workhorse:job:index
     */
    INC_JOB_ID("w:j:i"),

    /**
     * List of IDs of all jobs
     */
    LIST_OF_JOB("w:j:l"),

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
     * workhorse:job:status:<JobStatus>:list
     */
    LIST_OF_JOB_BY_STATUS("w:j:s:%s:l"),

    // KEYS FOR EXECUTION

    /**
     * Value to use as ID for execution.
     * 
     * workhorse:execution:index
     */
    INC_EXECUTION_ID("w:j:e:i"),

    /**
     * Store the execution with all attributes as JSON
     * 
     * workhorse:job:<JobId>:execution:<executionId>
     */
    EXECUTION_BY_ID("w:j:%s:e:%s"),

    /**
     * Store the executionId associated with the given parameterhash
     * 
     * workhorse:job:<JobId>:execution:paramHash:<parametersHash>
     */
    EXECUTION_BY_PARAMETER_HASH("w:j:%s:e:p:%s"),

    /**
     * List of all executionId by JobId
     * 
     * workhorse:job:<JobId>:execution:list
     */
    LIST_OF_EXECUTION_BY_JOB("w:j:%s:e:l"),

    /**
     * Queue to store new created executions
     */
    // EXECUTION_QUEUE("queue:exe"),

    // /**
    // * Queue to store new created executions
    // */
    // PRIORITY_EXECUTION_QUEUE("queue:exe:priority"),

    /**
     * List of all executionId by status
     * 
     * workhorse:job:<JobId>:execution:executionStatus:<status>:list
     */
    LIST_OF_EXECUTION_OF_JOB_BY_STATUS("w:j:%s:e:s:%s:l"),

    /**
     * List of all executionId by batchId
     * 
     * workhorse:job:<JobId>:execution:batch:<batchId>:list
     */
    LIST_OF_EXECUTION_OF_BATCH("w:j:%s:e:b:%s:l"),

    /**
     * List of all executionId by chainId
     * 
     * workhorse:job:<JobId>:execution:chain:<batchId>:list
     */
    LIST_OF_EXECUTION_OF_CHAIN("w:j:%s:e:c:%s:l"),

    // KEYS FOR EXECUTION LOG

    /**
     * Store the log execution with all attributes as JSON
     * 
     * workhorse:job:<JobId>:execution:<executionId>:log
     */
    EXECUTION_LOG_BY_ID("w:j:%s:e:%s:lg"),

    /**
     * Store the logs of an execution
     * 
     * workhorse:job:<JobId>:execution:<executionId>:log:list
     */
    LIST_OF_EXECUTION_LOG_BY_ID("w:j:%s:e:%s:lg:l"),

    /**
     * Store the error of an execution
     * 
     * workhorse:job:<JobId>:execution:<executionId>:errorLog:
     */
    EXECUTION_ERROR_AND_STACKTRACE_BY_ID("w:j:%s:e:%s:er"),

    // KEYS FOR WORKHORSE_LOG

    /**
     * Value to use as Id for JobEngineLog.
     * 
     * workhorse:log:index
     */
    INC_WORKHORSE_LOG_ID("w:lg:i"),

    /**
     * Store a workhorseLog with all attributes as JSON
     * 
     * workhorse:log:<workhorseLogId>
     */
    WORKHORSE_LOG_BY_ID("w:lg:%s"),

    /**
     * List of workhorse logs by jobId
     * 
     * workhorse:log:job:<jobId>:list
     */
    LIST_OF_WORKHORSE_LOG_BY_JOB("w:j:%s:lg:l"),

    /**
     * List of all workhorse logs
     * 
     * workhorse:log:list
     */
    WORKHORSE_LOG_LIST("w:lg:l");

    String query;

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
