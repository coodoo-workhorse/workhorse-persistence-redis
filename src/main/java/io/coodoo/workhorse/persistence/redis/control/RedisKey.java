package io.coodoo.workhorse.persistence.redis.control;

import java.util.Arrays;
import org.jboss.logging.Logger;

public enum RedisKey {

    SET_OF_JOB("set:job"),

    /**
     * List of all JobExecutionId by JobId Set:Job:<id>:JobExecutions
     */
    SET_OF_EXECUTION_BY_JOB("set:job:%s:jobExecutions"),

    /**
     * 
     */
    JOB_ENGINE_CONFIG("job:config"),

    /**
     * Job:<id>
     */
    JOB_BY_ID("job:%s"),

    /**
     * Value to use as Id for Job.
     */
    INC_JOB_ID("jobIndex"),

    /**
     * Value to use as Id for JobExecution.
     */
    INC_JOB_EXECUTION_ID("jobExectionIndex"),

    /**
     * Job:<id>:JobExecution:<id>
     */
    JOB_EXECUTION_BY_ID("job:%s:jobE:%s"),

    /**
     * Value to use as Id for JobEngineLog.
     */
    INC_JOB_ENGINE_LOG_ID("jobEngineLogIndex"),

    /**
     * jobEngineLog:<id>
     */
    JOB_ENGINE_LOG("jobEngineLog:%s"),

    JOB_ENGINE_LOG_LIST("jobEngineLogList");

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
