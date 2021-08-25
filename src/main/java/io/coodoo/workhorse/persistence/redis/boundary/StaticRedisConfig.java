package io.coodoo.workhorse.persistence.redis.boundary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public class StaticRedisConfig {

    // TODO wieso sind hier alle werte erneut definiert? das gleiche haben wir doch bereits in RedisPersistenceConfig

    public static final String NAME = "Redis Persistence";

    public static String REDIS_HOST = "localhost";

    public static int REDIS_PORT = 6379;

    public static int MAX_TOTAL = 10240;

    public static int MAX_IDLE = 100;

    public static int MIN_IDLE = 50;

    public static String version = null;
    {
        // TODO ebenfalls die gleiche logik wie in RedisPersistenceConfig
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

}
