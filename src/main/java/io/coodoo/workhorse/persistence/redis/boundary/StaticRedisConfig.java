package io.coodoo.workhorse.persistence.redis.boundary;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public class StaticRedisConfig {

    public static final String NAME = "Redis Persistence";

    public static final String UNSUBSCRIBE_MESSAGE = "Unsubscribe the pattern";
    /**
     * Host of the redis server
     */
    public static String REDIS_HOST;

    /**
     * Port of the redis server
     */
    public static int REDIS_PORT;

    /**
     * Timeout for a request in ms
     */
    public static int TIME_OUT;

    /**
     * The max number of connections permitted in the redis pool
     */
    public static int MAX_TOTAL;

    /**
     * The max number of connections that may idle in the redis pool
     */
    public static int MAX_IDLE;

    /**
     * The min number of connections that may idle in the redis pool
     */
    public static int MIN_IDLE;

    /**
     * Version of the current persistence
     */
    public static String version;

    private StaticRedisConfig() {
        throw new IllegalStateException("Utility class");
    }

}
