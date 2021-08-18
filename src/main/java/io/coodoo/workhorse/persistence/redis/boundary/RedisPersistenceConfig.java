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

    protected String persistenceName = "Redis Persistence";
    protected String redisHost = "localhost";
    protected int redisPort = 6379;
    protected int maxTotal = 10240;
    protected int maxIdle = 100;
    protected int minIdle = 50;

    protected String version = null;
    {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("workhorse-persistence-redis.txt");
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
        return StaticRedisConfig.NAME;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setPersistenceName(String persistenceName) {
        this.persistenceName = persistenceName;
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

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

}
