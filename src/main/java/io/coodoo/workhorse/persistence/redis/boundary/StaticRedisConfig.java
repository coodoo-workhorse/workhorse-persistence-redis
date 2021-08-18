package io.coodoo.workhorse.persistence.redis.boundary;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public class StaticRedisConfig {

    public static final String NAME = "Redis Persistence";

    public static String REDIS_HOST = "localhost";

    public static int REDIS_PORT = 6379;

    public static int MAX_TOTAL = 10240;

    public static int MAX_IDLE = 100;

    public static int MIN_IDLE = 50;
}
