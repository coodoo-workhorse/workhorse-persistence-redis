package io.coodoo.workhorse.persistence.redis;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseLog;
import io.coodoo.workhorse.persistence.interfaces.LogPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import io.coodoo.workhorse.persistence.redis.control.RedisClient;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.util.CollectionListing;
import io.coodoo.workhorse.util.WorkhorseUtil;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisLogPersistence implements LogPersistence {

    @Inject
    RedisClient redisClient;

    @Override
    public WorkhorseLog get(Long logId) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(logId);
        return redisClient.get(workhorseLogKey, WorkhorseLog.class);
    }

    @Override
    public WorkhorseLog update(Long workhorseLogId, WorkhorseLog workhorseLog) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisClient.set(workhorseLogKey, workhorseLog);

        return workhorseLog;
    }

    @Override
    public WorkhorseLog delete(Long logId) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(logId);

        WorkhorseLog log = redisClient.get(workhorseLogKey, WorkhorseLog.class);

        Long jobId = log.getJobId();

        if (jobId != null) {

            // remove the ID of the log in the list of workhorse IDs of the given jobId
            String workhorseLogByJobKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);
            redisClient.lrem(workhorseLogByJobKey, logId);
        }

        // remove the ID of the log in the global list of IDs
        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        redisClient.lrem(workhorseLogListKey, logId);

        // Delete the workhorse Log
        redisClient.del(workhorseLogKey);
        return log;
    }

    @Override
    public WorkhorseLog persist(WorkhorseLog workhorseLog) {

        Long workhorseLogId = redisClient.incr(RedisKey.INC_WORKHORSE_LOG_ID.getQuery());
        workhorseLog.setId(workhorseLogId);
        workhorseLog.setCreatedAt(WorkhorseUtil.timestamp());

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisClient.set(workhorseLogKey, workhorseLog);

        // add log to the list of logs
        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        redisClient.lpush(workhorseLogListKey, workhorseLogId);

        Long jobId = workhorseLog.getJobId();
        if (jobId != null) {
            // add log to the list of logs with the given job's ID
            String workhorseLogByJobListKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);
            redisClient.lpush(workhorseLogByJobListKey, workhorseLogId);
        }
        return workhorseLog;
    }

    @Override
    public List<WorkhorseLog> getAll(int limit) {
        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        List<Long> workhorseLogIds = redisClient.lrange(workhorseLogListKey, Long.class, 0, limit - 1);

        List<String> workhorseLogKeys = new ArrayList<>();
        for (Long workhorseLogId : workhorseLogIds) {
            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
            workhorseLogKeys.add(workhorseLogKey);
        }
        return redisClient.get(workhorseLogKeys, WorkhorseLog.class);
    }

    @Override
    public ListingResult<WorkhorseLog> getWorkhorseLogListing(ListingParameters listingParameters) {

        List<WorkhorseLog> workhorseLogs = getAll();
        return CollectionListing.getListingResult(workhorseLogs, WorkhorseLog.class, listingParameters);
    }

    // TODO warum ist die methode ausgelagert?
    protected List<WorkhorseLog> getAll() {

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        List<Long> workhorseLogIds = redisClient.lrange(workhorseLogListKey, Long.class, 0, -1);

        List<String> workhorseLogIdsKeys = new ArrayList<>();

        for (Long workhorseLogId : workhorseLogIds) {

            workhorseLogIdsKeys.add(RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId));
        }
        return redisClient.get(workhorseLogIdsKeys, WorkhorseLog.class);
    }

    @Override
    public int deleteByJobId(Long jobId) {

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();

        String workhorseLogByJobKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);

        List<Long> workhorseLogIds = redisClient.lrange(workhorseLogByJobKey, Long.class, 0, -1);

        for (Long workhorseLogId : workhorseLogIds) {

            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);

            // remove the ID of the log in the list of workhorse IDs of the given jobId
            redisClient.lrem(workhorseLogByJobKey, workhorseLogId);

            // remove the ID of the log in the global list of IDs
            redisClient.lrem(workhorseLogListKey, workhorseLogId);

            // Delete the workhorse Log
            redisClient.del(workhorseLogKey);
        }

        // delete the key of the list of workhorse IDs of the given jobId
        redisClient.del(workhorseLogByJobKey);

        return 0;
    }

    @Override
    public String getPersistenceName() {
        return StaticRedisConfig.NAME;
    }

    @Override
    public void connect(Object... params) {}

}
