package io.coodoo.workhorse.persistence.redis.entity;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseLog;
import io.coodoo.workhorse.persistence.interfaces.LogPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.interfaces.listing.Metadata;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.RedisController;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.util.WorkhorseUtil;

@ApplicationScoped
public class RedisLogPersistence implements LogPersistence {

    @Inject
    RedisController redisService;

    @Inject
    JedisExecution jedisExecution;

    @Override
    public WorkhorseLog get(Long logId) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(logId);
        return redisService.get(workhorseLogKey, WorkhorseLog.class);
    }

    @Override
    public WorkhorseLog update(Long workhorseLogId, WorkhorseLog workhorseLog) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisService.set(workhorseLogKey, workhorseLog);

        return workhorseLog;
    }

    @Override
    public WorkhorseLog delete(Long logId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseLog persist(WorkhorseLog workhorseLog) {

        Long workhorseLogId = redisService.incr(RedisKey.INC_WORKHORSE_LOG_ID.getQuery());
        workhorseLog.setId(workhorseLogId);
        workhorseLog.setCreatedAt(WorkhorseUtil.timestamp());

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisService.set(workhorseLogKey, workhorseLog);

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        redisService.lpush(workhorseLogListKey, workhorseLogId);

        Long jobId = workhorseLog.getJobId();
        if (jobId != null) {
            String workhorseLogByJobListKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);
            redisService.lpush(workhorseLogByJobListKey, workhorseLog);
        }

        return workhorseLog;
    }

    @Override
    public List<WorkhorseLog> getAll(int limit) {
        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        List<Long> workhorseLogIds = redisService.lrange(workhorseLogListKey, Long.class, 0, limit);

        List<String> workhorseLogKeys = new ArrayList<>();
        for (Long workhorseLogId : workhorseLogIds) {
            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
            workhorseLogKeys.add(workhorseLogKey);
        }

        List<WorkhorseLog> workhorseLogs = redisService.get(workhorseLogKeys, WorkhorseLog.class);

        return workhorseLogs;
    }

    @Override
    public ListingResult<WorkhorseLog> getWorkhorseLogListing(ListingParameters listingParameters) {

        long start = listingParameters.getIndex();
        long end = listingParameters.getIndex() + listingParameters.getLimit() - 1;

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        List<Long> workhorseLogIds = redisService.lrange(workhorseLogListKey, Long.class, start, end);

        List<String> executionIdKeys = new ArrayList<>();
        List<WorkhorseLog> result = new ArrayList<>();

        for (Long workhorseLogId : workhorseLogIds) {

            executionIdKeys.add(RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId));
        }

        result = redisService.get(executionIdKeys, WorkhorseLog.class);

        long size = redisService.llen(workhorseLogListKey);
        Metadata metadata = new Metadata(size, listingParameters);

        return new ListingResult<WorkhorseLog>(result, metadata);
    }

    @Override
    public int deleteByJobId(Long jobId) {

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();

        String workhorseLogByJobKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);

        List<Long> workhorseLogIds = redisService.lrange(workhorseLogByJobKey, Long.class, 0, -1);

        List<String> executionIdKeys = new ArrayList<>();
        for (Long workhorseLogId : workhorseLogIds) {

            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
            // add the redis key of the workhorse log to the list of keys to delete
            executionIdKeys.add(workhorseLogKey);

            // remove the ID of the log in the list of workhorse IDs of the given jobId
            redisService.lrem(workhorseLogByJobKey, workhorseLogId);

            // remove the ID of the log in the global list of IDs
            redisService.lrem(workhorseLogListKey, workhorseLogId);

            // Delete the workhorse Log
            redisService.del(workhorseLogKey);
        }

        // delete the key of the list of workhorse IDs of the given jobId
        redisService.del(workhorseLogByJobKey);

        return 0;
    }

    @Override
    public String getPersistenceName() {
        return RedisPersistenceConfig.NAME;
    }

    @Override
    public void connect(Object... params) {
        // TODO Auto-generated method stub

    }

}
