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
    public WorkhorseConfig get() {

        RedisPersistenceConfig redisPersistenceConfig = redisService.get(RedisKey.CONFIG.getQuery(), RedisPersistenceConfig.class);

        if (redisPersistenceConfig == null) {
            return null;
        }
        // TODO warum nicht einfach so?! --> return (WorkhorseConfig) redisPersistenceConfig;

        WorkhorseConfig workhorseConfig = new RedisPersistenceConfig();
        workhorseConfig.setTimeZone(redisPersistenceConfig.getTimeZone());
        workhorseConfig.setBufferMax(redisPersistenceConfig.getBufferMax());
        workhorseConfig.setBufferMin(redisPersistenceConfig.getBufferMin());
        workhorseConfig.setBufferPollInterval(redisPersistenceConfig.getBufferPollInterval());
        workhorseConfig.setBufferPushFallbackPollInterval(redisPersistenceConfig.getBufferPushFallbackPollInterval());
        workhorseConfig.setExecutionTimeout(redisPersistenceConfig.getExecutionTimeout());
        workhorseConfig.setExecutionTimeoutStatus(redisPersistenceConfig.getExecutionTimeoutStatus());
        workhorseConfig.setMaxExecutionSummaryLength(redisPersistenceConfig.getMaxExecutionSummaryLength());
        workhorseConfig.setMinutesUntilCleanup(redisPersistenceConfig.getMinutesUntilCleanup());
        workhorseConfig.setLogChange(redisPersistenceConfig.getLogChange());
        workhorseConfig.setLogTimeFormat(redisPersistenceConfig.getLogTimeFormat());
        workhorseConfig.setLogInfoMarker(redisPersistenceConfig.getLogInfoMarker());
        workhorseConfig.setLogWarnMarker(redisPersistenceConfig.getLogWarnMarker());
        workhorseConfig.setLogErrorMarker(redisPersistenceConfig.getLogErrorMarker());

        return workhorseConfig;
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

    @Override
    public void connect(Object... params) {

        if (params == null || params.length == 0) {
            // TODO ich finde hier sollte es laut krachen. also eine exception fliegen, die kurz sagt, was faul ist. denn sonst könnte macn ja ein fach die
            // bedingung der nächsten abfrage hier mit aufnehmen.
            return;
        }

        if (!(params[0] instanceof RedisPersistenceConfig)) {
            // TODO ich finde hier sollte es laut krachen. also eine exception fliegen, die kurz sagt, was faul ist.
            return;
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

}
