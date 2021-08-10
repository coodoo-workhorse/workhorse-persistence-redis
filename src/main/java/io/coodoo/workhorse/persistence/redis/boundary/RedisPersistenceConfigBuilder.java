package io.coodoo.workhorse.persistence.redis.boundary;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;
import io.coodoo.workhorse.core.entity.WorkhorseConfigBuilder;

public class RedisPersistenceConfigBuilder extends WorkhorseConfigBuilder {

    private RedisPersistenceConfig config = new RedisPersistenceConfig();

    public RedisPersistenceConfigBuilder() {
        this.workhorseConfig = config;
    }

    @Override
    public WorkhorseConfig build() {
        return this.config;
    }

    public WorkhorseConfigBuilder redisHost(String redisHost) {
        config.setRedisHost(redisHost);
        return this;
    }

    public WorkhorseConfigBuilder redisPort(String redisPort) {
        config.setRedisHost(redisPort);
        return this;
    }

}
