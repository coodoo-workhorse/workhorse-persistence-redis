package io.coodoo.workhorse.persistence.redis;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.Execution;
import io.coodoo.workhorse.core.entity.ExecutionFailStatus;
import io.coodoo.workhorse.core.entity.ExecutionLog;
import io.coodoo.workhorse.core.entity.ExecutionStatus;
import io.coodoo.workhorse.core.entity.Job;
import io.coodoo.workhorse.core.entity.JobExecutionCount;
import io.coodoo.workhorse.core.entity.JobExecutionStatusSummary;
import io.coodoo.workhorse.persistence.interfaces.ExecutionPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import io.coodoo.workhorse.persistence.redis.control.RedisClient;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.persistence.redis.control.subscribe.ChannelToSubscribe;
import io.coodoo.workhorse.persistence.redis.control.subscribe.RedisPubSub;
import io.coodoo.workhorse.util.CollectionListing;
import io.coodoo.workhorse.util.WorkhorseUtil;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisExecutionPersistence implements ExecutionPersistence {

    @Inject
    RedisClient redisClient;

    @Inject
    RedisPubSub redisPubSub;

    @Inject
    Event<ChannelToSubscribe> channelToSubscribe;

    @Override
    public Execution getById(Long jobId, Long executionId) {

        if (jobId == null) {
            Set<String> executionKeys = redisClient.keys(RedisKey.EXECUTION_BY_ID.getQuery("*", executionId));

            for (String executioney : executionKeys) {
                Execution execution = redisClient.get(executioney, Execution.class);
                if (execution != null) {
                    return execution;
                }
            }
        }
        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
        return redisClient.get(executionKey, Execution.class);
    }

    @Override
    public List<Execution> getByJobId(Long jobId, Long limit) {
        String executionByJobKey = RedisKey.EXECUTION_BY_JOB_LIST.getQuery(jobId);

        List<Long> executionIds = redisClient.lrange(executionByJobKey, Long.class, 0, limit);

        List<String> executionIdsKeys = new ArrayList<>();

        for (Long executionId : executionIds) {

            executionIdsKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
        }
        return redisClient.get(executionIdsKeys, Execution.class);
    }

    @Override
    public ListingResult<Execution> getExecutionListing(Long jobId, ListingParameters listingParameters) {

        String redisKey = RedisKey.EXECUTION_BY_JOB_LIST.getQuery(jobId);

        List<Execution> result = new ArrayList<>();

        // TODO ;) These values are work in progress!!
        long start = 0;
        long end = 999;

        long startTime = System.currentTimeMillis();
        long time = 0l;
        long size = redisClient.llen(redisKey);
        while (time < 50l || result.size() < size) {

            List<String> executionIdKeys = new ArrayList<>();
            List<Long> executionIds = redisClient.lrange(redisKey, Long.class, start, end);
            for (Long executionId : executionIds) {

                executionIdKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
            }
            result.addAll(redisClient.get(executionIdKeys, Execution.class));

            start = end;
            // TODOX sollte es hier nicht um 1000 inkrementiert werden?
            end = start + 1000;
            time = System.currentTimeMillis() - startTime;
        }
        return CollectionListing.getListingResult(result, Execution.class, listingParameters);
    }

    @Override
    public List<Execution> pollNextExecutions(Long jobId, int limit) {

        LocalDateTime currentTimeStamp = WorkhorseUtil.timestamp();

        String listOfQueuedExecutionByJob = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(jobId, ExecutionStatus.QUEUED);
        String listOfPlannedExecutionByJob = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(jobId, ExecutionStatus.PLANNED);

        List<Execution> result = new ArrayList<>();

        long start = 0;
        long end = Long.valueOf(limit) - 1l;
        // Get the executions in status Queued
        List<Long> queuedExecutionId = redisClient.lrange(listOfQueuedExecutionByJob, Long.class, start, end);

        List<String> queuedExecutionIdsKeys = new ArrayList<>();

        for (Long executionId : queuedExecutionId) {
            queuedExecutionIdsKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
        }

        result.addAll(redisClient.get(queuedExecutionIdsKeys, Execution.class));

        // Get the executions in status Planned
        List<Long> plannedExecutionId = redisClient.lrange(listOfPlannedExecutionByJob, Long.class, start, end);

        List<String> plannedExecutionIdsKeys = new ArrayList<>();

        for (Long executionId : plannedExecutionId) {
            plannedExecutionIdsKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
        }

        for (Execution execution : redisClient.get(plannedExecutionIdsKeys, Execution.class)) {
            if (execution.getPlannedFor() != null && execution.getPlannedFor().isBefore(currentTimeStamp)) {
                result.add(execution);
            }
        }
        return result;
    }

    @Override
    public Execution persist(Execution execution) {
        Long executionId = redisClient.incr(RedisKey.EXECUTION_ID_INDEX.getQuery());
        Long jobId = execution.getJobId();
        execution.setId(executionId);
        execution.setCreatedAt(WorkhorseUtil.timestamp());

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
        redisClient.set(executionKey, execution);

        // add the execution to the list of execution of the job with the given ID
        String listOfExecutionByJobKey = RedisKey.EXECUTION_BY_JOB_LIST.getQuery(execution.getJobId());
        redisClient.lpush(listOfExecutionByJobKey, executionId);

        // Store the parameterHash as a key with the ID of the execution as value
        Integer parameterHash = execution.getParametersHash();
        if (parameterHash != null) {
            String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(jobId, parameterHash);
            redisClient.set(parameterHashKey, executionId);
        }

        // add the execution to the chain
        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.EXECUTION_OF_CHAIN_LIST.getQuery(jobId, chainId);
            redisClient.rpush(listOfExecutionOfChainId, executionId);
        }

        // add the execution to the batch
        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.EXECUTION_OF_BATCH_LIST.getQuery(jobId, batchId);
            redisClient.rpush(listOfExecutionOfBatchId, executionId);
        }

        // elements of an chained execution do not have to be in a queue
        if (chainId == null || chainId < 0l) {
            // add the execution to the list of the ones with the given status
            String listOfExecutionByJobOnStatus = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(execution.getJobId(), execution.getStatus());
            redisClient.rpush(listOfExecutionByJobOnStatus, executionId);
        }

        String channelId = RedisKey.QUEUE_CHANNEL.getQuery(execution.getJobId());
        redisClient.publish(channelId, execution);
        return redisClient.get(executionKey, Execution.class);
    }

    @Override
    public void delete(Long jobId, Long executionId) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
        Execution execution = redisClient.get(executionKey, Execution.class);

        // remove the parameterHash as a key with the ID of the execution as value
        Integer parameterHash = execution.getParametersHash();
        if (parameterHash != null) {
            String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(jobId, parameterHash);
            redisClient.del(parameterHashKey);
        }

        // remove the execution in the chain
        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.EXECUTION_OF_CHAIN_LIST.getQuery(jobId, chainId);
            redisClient.lrem(listOfExecutionOfChainId, executionId);
        }

        // remove the execution to the batch
        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.EXECUTION_OF_BATCH_LIST.getQuery(jobId, batchId);
            redisClient.lrem(listOfExecutionOfBatchId, executionId);
        }

        // remove the execution ID in the list of executions in the current status
        String executionStatusKey = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(execution.getStatus());
        redisClient.lrem(executionStatusKey, executionId);

        // remove the execution to the list of execution of the job with the given ID
        String listOfExecutionByJobKey = RedisKey.EXECUTION_BY_JOB_LIST.getQuery(jobId);
        redisClient.lrem(listOfExecutionByJobKey, executionId);

        // remove the execution
        redisClient.del(executionKey);
    }

    @Override
    public Execution update(Execution execution) {

        Long executionId = execution.getId();
        Long jobId = execution.getJobId();

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);

        Execution oldExecution = redisClient.get(executionKey, Execution.class);

        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.EXECUTION_OF_CHAIN_LIST.getQuery(jobId, chainId);
            redisClient.rpush(listOfExecutionOfChainId, executionId);
        }

        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.EXECUTION_OF_BATCH_LIST.getQuery(jobId, batchId);
            redisClient.rpush(listOfExecutionOfBatchId, executionId);
        }

        if (!Objects.equals(oldExecution.getStatus(), execution.getStatus())) {

            // remove the ID of the execution in the list of her old status
            String oldListOfExecutionByJobOnStatus = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(jobId, oldExecution.getStatus());
            // add the ID of the execution in the list of the new status
            String newListOfExecutionByJobOnStatus = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(jobId, execution.getStatus());

            redisClient.lmove(oldListOfExecutionByJobOnStatus, newListOfExecutionByJobOnStatus, executionId);

        }

        execution.setUpdatedAt(WorkhorseUtil.timestamp());
        redisClient.set(executionKey, execution);

        return execution;
    }

    @Override
    public Execution updateStatus(Long jobId, Long executionId, ExecutionStatus status, ExecutionFailStatus failStatus) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
        Execution execution = redisClient.get(executionKey, Execution.class);

        if (status.equals(execution.getStatus())) {
            return execution;
        }

        execution.setStatus(status);

        if (failStatus == null) {
            failStatus = ExecutionFailStatus.NONE;
        }
        execution.setFailStatus(failStatus);

        return update(execution);
    }

    @Override
    public int deleteOlderExecutions(Long jobId, LocalDateTime preDate) {

        int count = 0;
        // get all executions
        List<Execution> executions = getByJobId(jobId, -1l);

        for (Execution execution : executions) {

            if (execution.getCreatedAt().isBefore(preDate)) {
                delete(jobId, execution.getId());
                count++;
            }
        }
        return count;
    }

    private List<Execution> getBundle(Long jobId, String bundleKey) {

        // Get the executions in the bundle by key
        List<Long> bundleExecutionIds = redisClient.lrange(bundleKey, Long.class, 0, -1);

        List<String> bundleExecutionIdsKeys = new ArrayList<>();

        for (Long executionId : bundleExecutionIds) {

            bundleExecutionIdsKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
        }
        return redisClient.get(bundleExecutionIdsKeys, Execution.class);
    }

    @Override
    public List<Execution> getBatch(Long jobId, Long batchId) {

        String listOfExecutionOfBatchId = RedisKey.EXECUTION_OF_BATCH_LIST.getQuery(jobId, batchId);

        return getBundle(jobId, listOfExecutionOfBatchId);
    }

    @Override
    public List<Execution> getChain(Long jobId, Long chainId) {

        String listOfExecutionOfChainId = RedisKey.EXECUTION_OF_CHAIN_LIST.getQuery(jobId, chainId);

        return getBundle(jobId, listOfExecutionOfChainId);
    }

    @Override
    public Execution getFirstCreatedByJobIdAndParametersHash(Long jobId, Integer parameterHash) {
        String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(jobId, parameterHash);

        Long executionId = redisClient.get(parameterHashKey, Long.class);

        if (executionId == null) {
            return null;
        }

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
        Execution execution = redisClient.get(executionKey, Execution.class);

        if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
            return execution;
        }
        return null;
    }

    @Override
    public boolean isBatchFinished(Long jobId, Long batchId) {

        // get the IDs of all executions of the given batch
        String listOfBatchKey = RedisKey.EXECUTION_OF_BATCH_LIST.getQuery(jobId, batchId);
        List<Long> batchExecutionIds = redisClient.lrange(listOfBatchKey, Long.class, 0, -1);

        // build the rediskeys used to get execution by ID
        List<String> executionKeys = new ArrayList<>();
        for (Long executionId : batchExecutionIds) {
            executionKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId));
        }

        // get all executions of this batch
        List<Execution> executions = redisClient.get(executionKeys, Execution.class);

        // if at least one execution is queued, the batch is not finished
        for (Execution execution : executions) {
            if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean abortChain(Long jobId, Long chainId) {

        // get the IDs of all executions of the given chain
        String listOfExecutionOfChainId = RedisKey.EXECUTION_OF_CHAIN_LIST.getQuery(jobId, chainId);
        List<Long> chainExecutionIds = redisClient.lrange(listOfExecutionOfChainId, Long.class, 0, -1);

        // set the queued executions of the chain to status ABORTED
        for (Long executionId : chainExecutionIds) {
            String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(jobId, executionId);
            Execution execution = redisClient.get(executionKey, Execution.class);
            if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
                updateStatus(jobId, executionId, ExecutionStatus.ABORTED, ExecutionFailStatus.NONE);
            }
        }
        return true;
    }

    @Override
    public List<Execution> findTimeoutExecutions(LocalDateTime time) {
        String redisKey = RedisKey.JOB_LIST.getQuery();

        List<Execution> result = new ArrayList<>();
        List<Long> jobIds = redisClient.lrange(redisKey, Long.class, 0, -1);

        for (Long jobId : jobIds) {

            List<Execution> executions = getByJobId(jobId, -1l);

            for (Execution execution : executions) {
                if (ExecutionStatus.RUNNING.equals(execution.getStatus()) && execution.getUpdatedAt().isBefore(time)) {
                    result.add(execution);
                }
            }
        }
        return result;
    }

    @Override
    public List<JobExecutionStatusSummary> getJobExecutionStatusSummaries(ExecutionStatus status, LocalDateTime since) {

        // The parameter -since- is not considered for to performance purpose with redis

        List<JobExecutionStatusSummary> result = new ArrayList<>();

        String listOfJobsKey = RedisKey.JOB_LIST.getQuery();

        List<Long> jobIds = redisClient.lrange(listOfJobsKey, Long.class, 0, -1);

        for (Long jobId : jobIds) {
            String listOfExecutionIdInStatus = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(jobId, status);

            List<Long> executionIds = redisClient.lrange(listOfExecutionIdInStatus, Long.class, 0, -1);

            if (executionIds != null && !executionIds.isEmpty()) {
                String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
                Job job = redisClient.get(jobKey, Job.class);
                result.add(new JobExecutionStatusSummary(status, Long.valueOf(executionIds.size()), job));
            }
        }
        return result;
    }

    @Override
    public JobExecutionCount getJobExecutionCount(Long jobId, LocalDateTime from, LocalDateTime to) {

        // The parameter -from- and -to- are not considered for to performance purpose with redis

        List<Long> jobIds = new ArrayList<>();

        if (jobId == null) {
            String listOfJobsKey = RedisKey.JOB_LIST.getQuery();

            List<Long> redisJobIds = redisClient.lrange(listOfJobsKey, Long.class, 0, -1);
            jobIds.addAll(redisJobIds);
        } else {
            jobIds.add(jobId);
        }

        long countPlanned = 0L;
        long countRunning = 0L;
        long countFinished = 0L;
        long countFailed = 0L;
        long countAbort = 0L;
        long countQueued = 0L;

        for (Long id : jobIds) {

            String listOfExecutionsPlanned = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.PLANNED);
            countPlanned = countPlanned + redisClient.llen(listOfExecutionsPlanned);

            String listOfExecutionsQueued = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.QUEUED);
            countQueued = countQueued + redisClient.llen(listOfExecutionsQueued);

            String listOfExecutionsRunning = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.RUNNING);
            countRunning = countRunning + redisClient.llen(listOfExecutionsRunning);

            String listOfExecutionsFinished = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.FINISHED);
            countFinished = countFinished + redisClient.llen(listOfExecutionsFinished);

            String listOfExecutionsFailed = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.FAILED);
            countFailed = countFailed + redisClient.llen(listOfExecutionsFailed);

            String listOfExecutionsAborted = RedisKey.EXECUTION_OF_JOB_BY_STATUS_LIST.getQuery(id, ExecutionStatus.ABORTED);
            countAbort = countAbort + redisClient.llen(listOfExecutionsAborted);
        }
        return new JobExecutionCount(jobId, from, to, countPlanned, countQueued, countRunning, countFinished, countFailed, countAbort);
    }

    @Override
    public ExecutionLog getLog(Long jobId, Long executionId) {

        String executionErrorKey = RedisKey.EXECUTION_ERROR_AND_STACKTRACE_BY_ID.getQuery(jobId, executionId);
        String executionLogKey = RedisKey.EXECUTION_LOG_BY_ID_LIST.getQuery(jobId, executionId);

        ExecutionLog executionLog = new ExecutionLog();
        executionLog.setId(executionId);
        executionLog.setExecutionId(executionId);

        ExecutionLog executionError = redisClient.get(executionErrorKey, ExecutionLog.class);

        if (executionError != null) {

            executionLog.setError(executionError.getError());
            executionLog.setStacktrace(executionError.getStacktrace());
        }

        // get all logs of the list
        List<String> executionLogs = redisClient.lrange(executionLogKey, String.class, 0, -1);
        StringBuilder bufferlog = new StringBuilder();

        // append the logs of the list to build one string
        for (String exelog : executionLogs) {
            bufferlog.append(exelog);
            bufferlog.append(System.lineSeparator());
        }
        // set the builded string to ExecutionLog.log
        executionLog.setLog(bufferlog.toString());

        return executionLog;
    }

    @Override
    public void log(Long jobId, Long executionId, String log) {

        String executionLogKey = RedisKey.EXECUTION_LOG_BY_ID_LIST.getQuery(jobId, executionId);

        redisClient.lpush(executionLogKey, log);
    }

    @Override
    public void log(Long jobId, Long executionId, String error, String stacktrace) {

        String executionLogKey = RedisKey.EXECUTION_ERROR_AND_STACKTRACE_BY_ID.getQuery(jobId, executionId);

        ExecutionLog executionLog = redisClient.get(executionLogKey, ExecutionLog.class);

        if (executionLog == null) {

            executionLog = new ExecutionLog();
            executionLog.setId(executionId);
            executionLog.setExecutionId(executionId);
            executionLog.setCreatedAt(WorkhorseUtil.timestamp());
        } else {
            executionLog.setUpdatedAt(WorkhorseUtil.timestamp());
        }

        executionLog.setError(error);
        executionLog.setStacktrace(stacktrace);

        redisClient.set(executionLogKey, executionLog);
    }

    @Override
    public void connect(Object... params) {}

    @Override
    public void subscribe() {

        // Subscribe the channels of all jobs
        String channelId = RedisKey.QUEUE_CHANNEL.getQuery("*");
        channelToSubscribe.fireAsync(new ChannelToSubscribe(channelId));
    }

    public void unsubscribe() {
        // unsubscribe the channels of all jobs
        String channelId = RedisKey.QUEUE_CHANNEL.getQuery("*");
        redisPubSub.punsubscribe(channelId);
    }

    @Override
    public String getPersistenceName() {
        return StaticRedisConfig.NAME;
    }

    @Override
    public boolean isPusherAvailable() {
        return true;
    }

}
