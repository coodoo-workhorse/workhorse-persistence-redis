package io.coodoo.workhorse.persistence.redis.boundary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public class RedisPersistenceConfig extends WorkhorseConfig {

    public static final String NAME = "Redis Persistence";

    public static String redisHost = "localhost";
    public static int redisPort = 6379;
    public static final int CACHE_TIME_1_HOURS = 60;
    public static final int CACHE_TIME_2_HOURS = 120;
    public static final int CACHE_TIME_4_HOURS = 240;
    public static final int CACHE_TIME_12_HOURS = 720;
    public static final int CACHE_TIME_24_HOURS = 1440;
    public static int standardCacheTime = CACHE_TIME_4_HOURS;

    private static String version = null;
    {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("workhorse-persistence-legacy.txt");
            InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);
            version = reader.readLine();
            if (version == null) {
                version = "Unknown";
            } else {
                if (version.endsWith("SNAPSHOT")) {
                    String timestamp = reader.readLine();
                    if (timestamp != null) {
                        version += " (" + timestamp + ")";
                    }
                }
            }
        } catch (IOException e) {
            version = "Unknown (" + e.getMessage() + ")";
        }
    }

    public RedisPersistenceConfig() {}

    @Override
    public String getPersistenceName() {
        return NAME;
    }

    @Override
    public String getPersistenceVersion() {
        return version;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public Integer getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

}
