package io.coodoo.workhorse.persistence.redis.control;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Destroyed;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

@ApplicationScoped
public class JedisExecution {

    private static final Logger log = LoggerFactory.getLogger(JedisExecution.class);

    private static RedisPersistenceConfig redisPersistenceConfig = new RedisPersistenceConfig();

    private JedisPool jedisPool;

    public void init(@Observes @Initialized(ApplicationScoped.class) Object init) {
        if (jedisPool == null) {
            init();
        }
    }

    public void init() {
        log.info("Creating Redis-Pool mit HOST: " + RedisPersistenceConfig.redisHost + ": " + RedisPersistenceConfig.redisPort);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10240);
        poolConfig.setMinIdle(50);
        poolConfig.setMaxIdle(100);

        jedisPool = new JedisPool(poolConfig, RedisPersistenceConfig.redisHost, RedisPersistenceConfig.redisPort, 1200);

    }

    public <T> T execute(JedisOperation<T> operation) {

        if (jedisPool.getNumActive() > 500) {
            log.info("Jedis Pool Stats. Active: {}, Idle: {}, Waiters: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(), jedisPool.getNumWaiters());
        }
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return operation.perform(jedis);
        } catch (JedisConnectionException e) {
            log.error("JedisConnectionException bei execute.", e);
            if (null != jedis) {
                jedis.close();
                jedis = null;
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return null;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void destroy(@Observes @Destroyed(ApplicationScoped.class) Object init) {

        log.info("Close Redis-Pool / Connections: Active: {}, Idle: {}, Waiters: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(),
                        jedisPool.getNumWaiters());

        jedisPool.destroy();
    }

}
