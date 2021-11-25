package io.coodoo.workhorse.persistence.redis.boundary;

import io.coodoo.workhorse.core.entity.WorkhorseConfigBuilder;

/**
 * Class to build an object of type {@link RedisPersistenceConfig}
 * 
 * @author coodoo GmbH (coodoo.io)
 */
public class RedisPersistenceConfigBuilder extends WorkhorseConfigBuilder {

    private RedisPersistenceConfig redisPersistenceConfig = new RedisPersistenceConfig();

    public RedisPersistenceConfigBuilder() {
        this.workhorseConfig = redisPersistenceConfig;
    }

    /**
     * Build an {@link RedisPersistenceConfig} object
     */
    @Override
    public RedisPersistenceConfig build() {
        return this.redisPersistenceConfig;
    }

    /**
     * Set the URI of the redis server. <br>
     * 
     * The URI has priority before the host and port defined with the builder.
     * 
     * @param redisUri host of the redis server
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder redisUri(String redisUri) {
        redisPersistenceConfig.setRedisUri(redisUri);
        return this;
    }

    /**
     * Set the host of the redis server
     * 
     * @param redisHost host of the redis server
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder redisHost(String redisHost) {
        redisPersistenceConfig.setRedisHost(redisHost);
        return this;
    }

    /**
     * Set the timeout for a request in ms
     * 
     * @param timeout timeout for a request in ms
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder timeout(int timeout) {
        redisPersistenceConfig.setTimeOut(timeout);
        return this;
    }

    /**
     * Set the port of the redis server
     * 
     * @param redisPort port of the redis server
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder redisPort(int redisPort) {
        redisPersistenceConfig.setRedisPort(redisPort);
        return this;
    }

    /**
     * Set the password of the redis server
     * 
     * @param redisPassword password of the redis server
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder redisPassword(String redisPassword) {
        redisPersistenceConfig.setRedisPassword(redisPassword);
        return this;
    }

    /**
     * Set the max number of connections permitted in the redis pool
     * 
     * @param maxTotal max number of connections permitted in the redis pool
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder maxTotal(int maxTotal) {
        redisPersistenceConfig.setMaxTotal(maxTotal);
        return this;
    }

    /**
     * Set the max number of connections that may idle in the redis pool
     * 
     * @param maxIdle max number of connections that may idle in the redis pool
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder maxIdle(int maxIdle) {
        redisPersistenceConfig.setMaxIdle(maxIdle);
        return this;
    }

    /**
     * Set the min number of connections that may idle in the redis pool
     * 
     * @param minIdle min number of connections that may idle in the redis pool
     * @return the builder to set another configuration
     */
    public RedisPersistenceConfigBuilder minIdle(int minIdle) {
        redisPersistenceConfig.setMinIdle(minIdle);
        return this;
    }

}
