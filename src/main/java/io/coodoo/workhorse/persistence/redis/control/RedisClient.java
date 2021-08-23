package io.coodoo.workhorse.persistence.redis.control;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import redis.clients.jedis.Jedis;
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

    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    @PostConstruct
    private void init() {
        jsonObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        jsonObjectMapper.registerModule(new JavaTimeModule());
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

        log.info("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);

        return retVal;
    }

    public <T> T hGet(String key, String field, Class<T> clazz) {

        Long t1 = System.currentTimeMillis();

        T retVal = jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {

                String value = jedis.hget(key, field);
                if (value == null) {
                    return null;
                }

                try {
                    return jsonObjectMapper.convertValue(value, clazz);
                } catch (IllegalArgumentException e) {
                    log.error("String could not deserialize object to  " + clazz + ": " + value, e);
                }
                return null;
            }

        });

        log.info("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);
        return retVal;
    }

    public <T> T hGet(String key, Class<T> clazz) {
        Long t1 = System.currentTimeMillis();

        T retVal = jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {

                Map<String, String> map = jedis.hgetAll(key);
                if (map == null) {
                    return null;
                }

                try {
                    return jsonObjectMapper.convertValue(map, clazz);
                } catch (IllegalArgumentException e) {
                    log.error("MAP could not deserialize object to  " + clazz + ": " + map, e);
                }
                return null;
            }

        });

        log.info("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);
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
                    return jsonObjectMapper.readValue(objectJson, clazz);
                } catch (IOException e) {
                    log.error("JSON could not deserialize object to  " + clazz + ": " + objectJson, e);
                }
                return null;
            }
        });

        log.info("Redis Call: {} {}ms", key, System.currentTimeMillis() - t1);

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
                            datas.add(jsonObjectMapper.readValue(rawJson, clazz));
                        }
                    } catch (IOException e) {
                        log.error("JSON could not deserialize object to  " + clazz + ": " + jsonData, e);
                    }
                }
                return datas;
            }
        });

        log.info("Redis Call: {} {}ms", keys, System.currentTimeMillis() - t1);

        return retVal;
    }

    public Boolean set(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.set(key, jsonObjectMapper.writeValueAsString(data));

                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    public Boolean hSet(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.hset(key, jsonObjectMapper.convertValue(data, new TypeReference<Map<String, String>>() {}));

                } catch (IllegalArgumentException e) {
                    log.error("Data could not be serialized to MAP: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    public Boolean setexExpirationInSeconds(String key, Long expirationInSeconds, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.setex(key, expirationInSeconds, jsonObjectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    /**
     * Saves a JSON serialized object with the SET method (no deletion of data after n minutes).
     */
    public Boolean setNoExpire(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.set(key, jsonObjectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }

                return true;
            }
        });
    }

    /**
     * Saves a string with the SET method (no deletion of the data after n minutes).
     */
    public Boolean setNoExpire(String key, String data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                jedis.set(key, data);
                return true;
            }
        });
    }

    /**
     * Adds a JSON serialized object to a SET with the SADD command.
     */
    public Boolean sadd(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.sadd(key, jsonObjectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }

                return true;
            }
        });
    }

    /**
     * Adds a string to a SET with the SADD command.
     */
    public Boolean sadd(String key, String data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                jedis.sadd(key, data);
                return true;
            }
        });
    }

    /**
     * Removes a string from a set with the SREM command.
     * 
     * return <code> true </code> if the element was part of the set, <code> false </code> if it did not appear in the set.
     */
    public boolean srem(String key, String... members) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                return jedis.srem(key, members) == 1;
            }
        });
    }

    /**
     * Returns all string members of this SET with the command SMBEMBERS.
     */
    public Set<String> smembers(String key) {
        return jedisExecution.execute(new JedisOperation<Set<String>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Set<String> perform(Jedis jedis) {
                return jedis.smembers(key);
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
     * Test if the specified key exists. The command returns <code>true</code> if the key exists, otherwise <code>false</code> is returned. Note that even keys
     * set with an empty string as value will return <code>true</code>. Time complexity: O(1)
     * 
     * @param key
     * @return Boolean reply, true if the key exists, otherwise false
     */
    public boolean exists(String key) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                return jedis.exists(key);
            }
        });
    }

    public Boolean flushAll() {
        log.info("Redis flush all..");
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                jedis.flushAll();
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

    public <T> Boolean createList(String key, List<T> datas) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                Pipeline pipeline = jedis.pipelined();
                // Liste erst löschen, falls bereits vorhanden
                pipeline.del(key);

                for (T data : datas) {
                    try {
                        String json = jsonObjectMapper.writeValueAsString(data);
                        pipeline.rpush(key, json);
                    } catch (JsonProcessingException e) {
                        log.error("JSON could not be created from {}: " + data, e);
                        return false;
                    }
                }
                pipeline.sync();
                return true;
            }
        });
    }

    public <T> Long rpush(String key, List<T> objects) {
        return jedisExecution.execute(new JedisOperation<Long>() {
            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                long count = 0;
                Pipeline pipeline = jedis.pipelined();
                for (T object : objects) {
                    try {
                        String json = jsonObjectMapper.writeValueAsString(object);
                        pipeline.rpush(key, json);
                        count++;
                    } catch (JsonProcessingException e) {
                        log.error("JSON could not be created from object in list {} ", key, e);
                    }
                }
                pipeline.sync();
                return count;
            }
        });
    }

    public Boolean rpush(String key, Object data) {
        return jedisExecution.execute(new JedisOperation<Boolean>() {
            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {
                try {
                    jedis.rpush(key, jsonObjectMapper.writeValueAsString(data));
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
                    jedis.lpush(key, jsonObjectMapper.writeValueAsString(data));
                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
                return true;
            }
        });
    }

    public <T> T lpop(String key, Class<T> clazz) {

        return jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {
                String objectJson = jedis.lpop(key);
                if (objectJson == null) {
                    return null;
                }
                try {
                    return jsonObjectMapper.readValue(objectJson, clazz);
                } catch (IOException e) {
                    log.error("JSON could not be deserialized to" + clazz + " object: " + objectJson, e);
                }
                return null;
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
                        datas.add(jsonObjectMapper.readValue(objectJson, clazz));
                    } catch (IOException e) {
                        log.error("JSON could not be deserialized to" + clazz + " object: " + objectJson, e);
                        return new ArrayList<>();
                    }
                }
                return datas;
            }
        });
    }

    /**
     * Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first element, 1 the second element and so on.
     * Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so
     * forth. When the value at key is not a list, an error is returned.
     * 
     * https://redis.io/commands/lindex
     */
    public <T> T lindex(String key, Class<T> clazz, long index) {

        return jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {
                String objectJson = jedis.lindex(key, index);
                if (objectJson == null) {
                    return null;
                }

                try {
                    return jsonObjectMapper.readValue(objectJson, clazz);
                } catch (IOException e) {
                    log.error("JSON konnte nicht zu " + clazz + " Objekt deserialisert werden: " + objectJson, e);
                }
                return null;
            }
        });
    }

    public long scard(String key) {
        return jedisExecution.execute(new JedisOperation<Long>() {
            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                return jedis.scard(key);
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
                    return jedis.lrem(key, 0, jsonObjectMapper.writeValueAsString(data));
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
                    transaction.lrem(srcListKey, 0, jsonObjectMapper.writeValueAsString(data));
                    transaction.rpush(destListKey, jsonObjectMapper.writeValueAsString(data));
                    transaction.exec();
                    return true;

                } catch (JsonProcessingException e) {
                    log.error("Data could not be serialized to JSON: " + data, e);
                    return false;
                }
            }
        });
    }

    public <T> void publish(String channelId, String message) {
        jedisExecution.execute(new JedisOperation<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T perform(Jedis jedis) {
                jedis.publish(channelId, message);
                return null;
            }
        });
    }
}
