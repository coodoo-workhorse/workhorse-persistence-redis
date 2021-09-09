package io.coodoo.workhorse.persistence.redis.control;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;

import io.coodoo.workhorse.persistence.redis.control.subscribe.RedisPubSub;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public class RedisPersistenceCDIExtension {

    public void register(@Observes BeforeBeanDiscovery bbdEvent) {
        bbdEvent.addAnnotatedType(JedisExecution.class, JedisExecution.class.getName());
        bbdEvent.addAnnotatedType(RedisPubSub.class, RedisPubSub.class.getName());
    }
}
