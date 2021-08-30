package io.coodoo.workhorse.persistence.redis.control;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@Dependent
public class RedisClient {

    private static final Logger log = LoggerFactory.getLogger(RedisClient.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    private void init() {
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Inject
    JedisExecution jedisExecution;

    public Long incr(String key) {
        return jedisExecution.execute(new JedisOperation<Long>() {
            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                return jedis.incr(key);
            }
        });
    }

    public Set<String> keys(String pattern) {
        return jedisExecution.execute(new JedisOperation<Set<String>>() {

            @SuppressWarnings("unchecked")
            public Set<String> perform(Jedis jedis) {
                return jedis.keys(pattern);
            }
        });
    }

    public String get(String key) {

        Long t1 = System.currentTimeMillis();
        String retVal = jedisExecution.execute(new JedisOperation<String>() {
            @SuppressWarnings("unchecked")
            @Override
            public String perform(Jedis jedis) {
                return jedis.get(key);
            }
        });

        log.debug("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);

        return retVal;
    }

    public <T> T get(String key, Class<T> clazz) {

        Long t1 = System.currentTimeMillis();

        T retVal = jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {
                String objectJson = jedis.get(key);
                if (objectJson == null) {
                    return null;
                }

                try {
                    return objectMapper.readValue(objectJson, clazz);
                } catch (IOException e) {
                    log.error("JSON could not deserialize object to  " + clazz + ": " + objectJson, e);
                }
                return null;
            }
        });

        log.debug("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);

        return retVal;
    }

    public <T> List<T> get(List<String> keys, Class<T> clazz) {

        Long t1 = System.currentTimeMillis();

        List<T> retVal = jedisExecution.execute(new JedisOperation<List<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public List<T> perform(Jedis jedis) {

                List<Response<String>> jsonDatas = new ArrayList<>();

                Pipeline pipeline = jedis.pipelined();

                for (String key : keys) {
                    Response<String> tippsJson = pipeline.get(key);
                    jsonDatas.add(tippsJson);
                }
                pipeline.sync();

                List<T> datas = new ArrayList<>();
                for (Response<String> jsonData : jsonDatas) {
                    try {
                        String rawJson = jsonData.get();
                        if (rawJson != null) {
                            datas.add(objectMapper.readValue(rawJson, clazz));
                        }
                    } catch (IOException e) {
                        log.error("JSON could not deserialize object to  " + clazz + ": " + jsonData, e);
                    }
                }
                return datas;
            }
        });

        log.debug("Redis Call: {} {}ms", keys, System.currentTimeMillis() - t1);

        return retVal;
    }

    public Boolean set(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.set(key, objectMapper.writeValueAsString(data));

                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    /**
     * Deletes the key from the Redis store
     * 
     * <br>
     * <strong>With wildcard (*) {@link RedisClient # deleteKeys (String) deleteKeys} should be used!</strong>
     */
    public Boolean del(String key) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                jedis.del(key);
                return true;
            }
        });
    }

    /**
     * Enables the deletion of entire key areas using wildcards in the key (*)
     */
    public void deleteKeys(String pattern) {
        jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                Set<String> matchingKeys = new HashSet<>();
                ScanParams params = new ScanParams();
                params.match(pattern);

                String nextCursor = "0";

                do {
                    ScanResult<String> scanResult = jedis.scan(nextCursor, params);
                    List<String> keys = scanResult.getResult();
                    nextCursor = scanResult.getCursor();
                    matchingKeys.addAll(keys);
                } while (!nextCursor.equals("0"));

                if (!matchingKeys.isEmpty()) {
                    jedis.del(matchingKeys.toArray(new String[matchingKeys.size()]));
                }
                return true;
            }
        });
    }

    public Boolean rpush(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.rpush(key, objectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    public Boolean lpush(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.lpush(key, objectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    public <T> List<T> lrange(String key, Class<T> clazz, long start, long end) {

        return jedisExecution.execute(new JedisOperation<List<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public List<T> perform(Jedis jedis) {
                List<String> list = jedis.lrange(key, start, end);
                if (list.isEmpty()) {
                    return new ArrayList<>();
                }

                List<T> datas = new ArrayList<>();

                for (String objectJson : list) {
                    try {
                        datas.add(objectMapper.readValue(objectJson, clazz));
                    } catch (IOException e) {
                        log.error("JSON could not be deserialized to" + clazz + " object: " + objectJson, e);
                        return new ArrayList<>();
                    }
                }
                return datas;
            }
        });
    }

    public long llen(String key) {
        return jedisExecution.execute(new JedisOperation<Long>() {
            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                return jedis.llen(key);
            }
        });
    }

    public long lrem(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Long>() {
            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                try {
                    return jedis.lrem(key, 0, objectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return null;
                }
            }
        });
    }

    public boolean lmove(String srcListKey, String destListKey, Object data) {

        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                Transaction transaction = jedis.multi();
                try {
                    transaction.lrem(srcListKey, 0, objectMapper.writeValueAsString(data));
                    transaction.rpush(destListKey, objectMapper.writeValueAsString(data));
                    transaction.exec();
                    return true;

                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
            }
        });
    }

    public boolean publish(String channelId, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {

                try {
                    jedis.publish(channelId, objectMapper.writeValueAsString(data));
                    return true;
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
            }
        });
    }

    public boolean psubscribe(JedisPubSub jedisPubSub, String channelPattern) {

        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {

                jedis.psubscribe(jedisPubSub, channelPattern);
                return true;
            }
        });

    }

}
