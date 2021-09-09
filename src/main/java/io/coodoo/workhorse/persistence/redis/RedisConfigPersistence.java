package io.coodoo.workhorse.persistence.redis;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;
import io.coodoo.workhorse.persistence.interfaces.ConfigPersistence;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.RedisClient;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisConfigPersistence implements ConfigPersistence {

    @Inject
    JedisExecution jedisExecution;

    @Inject
    RedisClient redisService;

    @Override
    public void initialize(Object... params) {

        if (params == null || params.length == 0) {
            throw new RuntimeException("No configuration of a redis server found. The redis persistence can not start.");
        }

        if (!(params[0] instanceof RedisPersistenceConfig)) {
            throw new RuntimeException("The parameter passed is not an instance of RedisPersistenceConfig. The redis persistence can not start.");
        }

        RedisPersistenceConfig redisPersistenceConfig = (RedisPersistenceConfig) params[0];
        StaticRedisConfig.REDIS_HOST = redisPersistenceConfig.getRedisHost();
        StaticRedisConfig.REDIS_PORT = redisPersistenceConfig.getRedisPort();
        StaticRedisConfig.TIME_OUT = redisPersistenceConfig.getTimeOut();
        StaticRedisConfig.MAX_TOTAL = redisPersistenceConfig.getMaxTotal();
        StaticRedisConfig.MAX_IDLE = redisPersistenceConfig.getMaxIdle();
        StaticRedisConfig.MIN_IDLE = redisPersistenceConfig.getMinIdle();

        jedisExecution.init();
    }

    @Override
    public WorkhorseConfig get() {

        return redisService.get(RedisKey.CONFIG.getQuery(), RedisPersistenceConfig.class);
    }

    @Override
    public WorkhorseConfig update(WorkhorseConfig workhorseConfig) {

        RedisPersistenceConfig redisPersisitenceConfig = new RedisPersistenceConfig();
        redisPersisitenceConfig.setTimeZone(workhorseConfig.getTimeZone());
        redisPersisitenceConfig.setBufferPollInterval(workhorseConfig.getBufferPollInterval());
        redisPersisitenceConfig.setBufferPushFallbackPollInterval(workhorseConfig.getBufferPushFallbackPollInterval());
        redisPersisitenceConfig.setBufferMax(workhorseConfig.getBufferMax());
        redisPersisitenceConfig.setBufferMin(workhorseConfig.getBufferMin());
        redisPersisitenceConfig.setMaxExecutionSummaryLength(workhorseConfig.getMaxExecutionSummaryLength());
        redisPersisitenceConfig.setExecutionTimeout(workhorseConfig.getExecutionTimeout());
        redisPersisitenceConfig.setExecutionTimeoutStatus(workhorseConfig.getExecutionTimeoutStatus());
        redisPersisitenceConfig.setMinutesUntilCleanup(workhorseConfig.getMinutesUntilCleanup());
        redisPersisitenceConfig.setLogChange(workhorseConfig.getLogChange());
        redisPersisitenceConfig.setLogTimeFormat(workhorseConfig.getLogTimeFormat());
        redisPersisitenceConfig.setLogInfoMarker(workhorseConfig.getLogInfoMarker());
        redisPersisitenceConfig.setLogWarnMarker(workhorseConfig.getLogWarnMarker());
        redisPersisitenceConfig.setLogErrorMarker(workhorseConfig.getLogErrorMarker());
        redisService.set(RedisKey.CONFIG.getQuery(), redisPersisitenceConfig);

        return get();
    }

    @Override
    public String getPersistenceName() {

        return StaticRedisConfig.NAME;
    }

    @Override
    public String getPersistenceVersion() {

        return new RedisPersistenceConfig().getPersistenceVersion();
    }

}
