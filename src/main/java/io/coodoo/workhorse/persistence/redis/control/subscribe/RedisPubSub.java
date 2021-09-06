package io.coodoo.workhorse.persistence.redis.control.subscribe;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.core.control.event.NewExecutionEvent;
import io.coodoo.workhorse.core.entity.Execution;
import io.coodoo.workhorse.util.WorkhorseUtil;
import redis.clients.jedis.JedisPubSub;

/**
 * Class for processing incomming messages from subcribed channels
 * 
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisPubSub extends JedisPubSub {

    private static final Logger log = LoggerFactory.getLogger(RedisPubSub.class);

    @Inject
    Event<NewExecutionEvent> newExecutionEventEvent;

    @Override
    public void onPMessage(String pattern, String channel, String message) {

        log.info("Channel {} has sent a message: {} on pattern {}", channel, message, pattern);

        try {
            Execution execution = WorkhorseUtil.jsonToParameters(message, Execution.class);
            newExecutionEventEvent.fireAsync(new NewExecutionEvent(execution.getJobId(), execution.getId()));
        } catch (Exception e) {
            log.error(" Error during recieving the message {} from channel {}. Exception: {}", message, channel, e.getMessage());
        }
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        log.info("Client subscribed to pattern: {}", pattern);
        log.info("Client subscribed to {} no. of patterns", subscribedChannels);
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        log.info("Client unsubscribed from pattern: {}", pattern);
        log.info("Client subscribed to {} no. of patterns", subscribedChannels);
    }
}
