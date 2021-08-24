package io.coodoo.workhorse.persistence.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.core.entity.Job;
import io.coodoo.workhorse.core.entity.JobStatus;
import io.coodoo.workhorse.core.entity.JobStatusCount;
import io.coodoo.workhorse.persistence.interfaces.JobPersistence;
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
public class RedisJobPersistence implements JobPersistence {

    private static Logger log = LoggerFactory.getLogger(RedisJobPersistence.class);

    @Inject
    RedisClient redisClient;

    @Override
    public Job get(Long jobId) {
        String redisKey = RedisKey.JOB_BY_ID.getQuery(jobId);
        return redisClient.get(redisKey, Job.class);
    }

    @Override
    public Job getByName(String jobName) {

        String jobNameKey = RedisKey.JOB_BY_NAME.getQuery(jobName);
        Long jobId = redisClient.get(jobNameKey, Long.class);

        if (jobId != null) {
            String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
            Job job = redisClient.get(jobKey, Job.class);
            if (job != null && jobName.equals(job.getName())) {
                return job;
            }
        }

        List<Job> jobs = getAll();

        for (Job job : jobs) {
            if (jobName.equals(job.getName())) {
                redisClient.set(jobNameKey, job);
                return job;
            }
        }

        return null;

    }

    @Override
    public Job getByWorkerClassName(String workerClassName) {

        String workerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(workerClassName);
        Long jobId = redisClient.get(workerNameKey, Long.class);

        if (jobId != null) {
            String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
            Job job = redisClient.get(jobKey, Job.class);
            if (job != null && workerClassName.equals(job.getWorkerClassName())) {
                return job;
            }
        }

        List<Job> jobs = getAll();

        for (Job job : jobs) {
            if (workerClassName.equals(job.getWorkerClassName())) {
                redisClient.set(workerNameKey, job);
                return job;
            }
        }

        return null;
    }

    @Override
    public List<Job> getAll() {

        String jobsListKey = RedisKey.LIST_OF_JOB.getQuery();
        List<Long> jobIds = redisClient.lrange(jobsListKey, Long.class, 0, -1);

        List<String> jobKeys = new ArrayList<>();

        for (Long id : jobIds) {
            jobKeys.add(RedisKey.JOB_BY_ID.getQuery(id));
        }

        return redisClient.get(jobKeys, Job.class);
    }

    @Override
    public ListingResult<Job> getJobListing(ListingParameters listingParameters) {

        long startTime = System.currentTimeMillis();
        Collection<Job> jobs = getAll();
        long duration = System.currentTimeMillis() - startTime;

        log.info("cachedJobs loaded in: {} ms", duration);

        return CollectionListing.getListingResult(jobs, Job.class, listingParameters);
    }

    @Override
    public List<Job> getAllByStatus(JobStatus jobStatus) {

        String jobListByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(jobStatus);
        List<Long> jobIdsByStatus = redisClient.lrange(jobListByStatusKey, Long.class, 0, -1);

        List<String> jobKeys = new ArrayList<>();

        for (Long id : jobIdsByStatus) {
            jobKeys.add(RedisKey.JOB_BY_ID.getQuery(id));
        }

        List<Job> resultJobs = new ArrayList<>();

        resultJobs.addAll(redisClient.get(jobKeys, Job.class));

        if (resultJobs.isEmpty()) {

            List<Job> jobs = getAll();

            for (Job job : jobs) {
                if (jobStatus.equals(job.getStatus())) {
                    resultJobs.add(job);
                    redisClient.lpush(jobListByStatusKey, job);
                }
            }
        }

        return resultJobs;
    }

    @Override
    public List<Job> getAllScheduled() {

        List<Job> resultJobs = new ArrayList<>();

        List<Job> jobs = getAll();

        for (Job job : jobs) {
            if (job.getSchedule() != null && !job.getSchedule().isEmpty()) {
                resultJobs.add(job);
            }
        }

        return resultJobs;
    }

    @Override
    public JobStatusCount getJobStatusCount() {

        long countActive = countByStatus(JobStatus.ACTIVE);

        long countInactive = countByStatus(JobStatus.INACTIVE);

        long countError = countByStatus(JobStatus.ERROR);

        long countNoWorker = countByStatus(JobStatus.NO_WORKER);

        return new JobStatusCount(countActive, countInactive, countNoWorker, countError);
    }

    @Override
    public Long count() {
        String jobsListKey = RedisKey.LIST_OF_JOB.getQuery();
        return redisClient.llen(jobsListKey);
    }

    @Override
    public Long countByStatus(JobStatus jobStatus) {
        String jobsByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(jobStatus);
        return redisClient.llen(jobsByStatusKey);
    }

    @Override
    public Job persist(Job job) {
        Long id = redisClient.incr(RedisKey.INC_JOB_ID.getQuery());
        String jobKey = RedisKey.JOB_BY_ID.getQuery(id);
        String jobNameKey = RedisKey.JOB_BY_NAME.getQuery(job.getName());
        String workerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(job.getWorkerClassName());
        String jobListKey = RedisKey.LIST_OF_JOB.getQuery();
        String jobListByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(job.getStatus());

        job.setId(id);
        job.setCreatedAt(WorkhorseUtil.timestamp());

        // add the job as JSON
        redisClient.set(jobKey, job);

        // add the ID of the job in to the job's name
        redisClient.set(jobNameKey, id);

        // add the ID of the job in to the workername
        redisClient.set(workerNameKey, id);

        // add the ID of the job in the list of jobs
        redisClient.lpush(jobListKey, id);

        // add the ID of the job to the list of job on the given status
        redisClient.lpush(jobListByStatusKey, id);

        return job;
    }

    // Think about put it in a redis transaction
    @Override
    public Job update(Job newJob) {

        Long jobId = newJob.getId();

        String jobKey = RedisKey.JOB_BY_ID.getQuery(newJob.getId());

        Job oldJob = redisClient.get(jobKey, Job.class);

        newJob.setUpdatedAt(WorkhorseUtil.timestamp());
        redisClient.set(jobKey, newJob);

        if (!Objects.equals(oldJob.getName(), newJob.getName())) {

            String oldJobNameKey = RedisKey.JOB_BY_NAME.getQuery(oldJob.getName());
            redisClient.del(oldJobNameKey);

            String newJobNameKey = RedisKey.JOB_BY_NAME.getQuery(newJob.getName());
            redisClient.set(newJobNameKey, jobId);
        }

        if (!Objects.equals(oldJob.getWorkerClassName(), newJob.getWorkerClassName())) {

            String oldWorkerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(oldJob.getWorkerClassName());
            redisClient.del(oldWorkerNameKey);

            String newWorkerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(newJob.getWorkerClassName());
            redisClient.set(newWorkerNameKey, jobId);
        }

        if (!Objects.equals(oldJob.getStatus(), newJob.getStatus())) {

            String jobListByOldStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(oldJob.getStatus());

            String jobListByNewStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(newJob.getStatus());

            redisClient.lmove(jobListByOldStatusKey, jobListByNewStatusKey, jobId);
        }

        return redisClient.get(jobKey, Job.class);
    }

    @Override
    public void deleteJob(Long jobId) {

        String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
        Job job = redisClient.get(jobKey, Job.class);

        String jobNameKey = RedisKey.JOB_BY_NAME.getQuery(job.getName());
        String workerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(job.getWorkerClassName());
        String jobListKey = RedisKey.LIST_OF_JOB.getQuery();
        String jobListByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(job.getStatus());

        deleteJobsLog(jobId);

        // remove the job from the list of job in the current status
        redisClient.lrem(jobListByStatusKey, jobId);

        // remove the job from the global list of job
        redisClient.lrem(jobListKey, jobId);

        // delete the index on the name of the job
        redisClient.del(jobNameKey);

        // delete the index on the name of the worker class of the job
        redisClient.del(workerNameKey);

        redisClient.del(jobKey);

        // delete all keys matching the pattern of the key of this job
        redisClient.deleteKeys(jobKey);
    }

    protected int deleteJobsLog(Long jobId) {

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();

        String workhorseLogByJobKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);

        List<Long> workhorseLogIds = redisClient.lrange(workhorseLogByJobKey, Long.class, 0, -1);

        List<String> executionIdKeys = new ArrayList<>();
        for (Long workhorseLogId : workhorseLogIds) {

            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
            // add the redis key of the workhorse log to the list of keys to delete
            executionIdKeys.add(workhorseLogKey);

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
    public void connect(Object... params) {}

    @Override
    public String getPersistenceName() {
        return StaticRedisConfig.NAME;
    }

}
