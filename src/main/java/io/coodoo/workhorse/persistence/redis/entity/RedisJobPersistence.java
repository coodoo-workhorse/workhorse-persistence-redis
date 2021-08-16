package io.coodoo.workhorse.persistence.redis.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.Job;
import io.coodoo.workhorse.core.entity.JobStatus;
import io.coodoo.workhorse.core.entity.JobStatusCount;
import io.coodoo.workhorse.persistence.interfaces.JobPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.interfaces.listing.Metadata;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.JedisOperation;
import io.coodoo.workhorse.persistence.redis.control.RedisController;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.util.WorkhorseUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@ApplicationScoped
public class RedisJobPersistence implements JobPersistence {

    @Inject
    RedisController redisService;

    @Inject
    JedisExecution jedisExecution;

    @Override
    public Job get(Long jobId) {
        String redisKey = RedisKey.JOB_BY_ID.getQuery(jobId);
        return redisService.hGet(redisKey, Job.class);
    }

    @Override
    public Job getByName(String jobName) {

        // User the map of job's name on job's ID to find the job
        String jobNameKey = RedisKey.JOB_BY_NAME.getQuery(jobName);

        Long jobId = redisService.get(jobNameKey, Long.class);

        if (jobId != null) {
            String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
            Job job = redisService.get(jobKey, Job.class);
            if (jobName.equals(job.getName())) {
                return job;
            }
        }

        // Get all the jobs and take the ones that have the wanted name
        Map<Long, Response<String>> responseMap = getAllJobsAsRedisResponse();

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);

            if (jobName.equals(job.getName())) {
                return job;
            }

        }

        return null;

    }

    @Override
    public Job getByWorkerClassName(String workerClassName) {

        // Use the map of worker class name on job's ID to find the job
        String workerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(workerClassName);

        Long jobId = redisService.get(workerNameKey, Long.class);

        if (jobId != null) {
            String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
            Job job = redisService.get(jobKey, Job.class);
            if (workerClassName.equals(job.getWorkerClassName())) {
                return job;
            }
        }

        // Get all the jobs and take the ones that have the wanted worker class name
        Map<Long, Response<String>> responseMap = getAllJobsAsRedisResponse();

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);

            if (workerClassName.equals(job.getWorkerClassName())) {
                return job;
            }

        }

        return null;
    }

    @Override
    public List<Job> getAll() {
        List<Job> resultJobs = new ArrayList<>();
        Map<Long, Response<String>> responseMap = getAllJobsAsRedisResponse();

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);

            resultJobs.add(job);
        }

        return resultJobs;
    }

    @Override
    public ListingResult<Job> getJobListing(ListingParameters listingParameters) {

        // String redisKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(jobId);

        // List<Long> executionIds = redisService.lrange(redisKey, Long.class, 0, -1);

        Map<Long, Response<String>> responseMap = getAllJobsAsRedisResponse();
        List<Job> result = new ArrayList<>();

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);

            result.add(job);

        }
        Metadata metadata = new Metadata(Long.valueOf(result.size()), listingParameters);

        return new ListingResult<Job>(result, metadata);
    }

    @Override
    public List<Job> getAllByStatus(JobStatus jobStatus) {

        String jobListByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(jobStatus);

        // Get all the jobs and take the ones that have the wanted status
        List<Job> resultJobs = new ArrayList<>();
        Map<Long, Response<String>> responseMap = new HashMap<Long, Response<String>>();

        // use the list of job in the given status
        List<Long> jobsByStatus = redisService.lrange(jobListByStatusKey, Long.class, 0, -1);

        if (jobsByStatus != null && !jobsByStatus.isEmpty()) {
            responseMap = getAllJobsAsRedisResponse(jobsByStatus);
        } else {
            responseMap = getAllJobsAsRedisResponse();
        }

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);

            if (jobStatus.equals(job.getStatus())) {
                resultJobs.add(job);
            }

        }

        return resultJobs;
    }

    @Override
    public List<Job> getAllScheduled() {

        List<Job> resultJobs = new ArrayList<>();
        Map<Long, Response<String>> responseMap = getAllJobsAsRedisResponse();

        for (Response<String> response : responseMap.values()) {

            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);
            if (job.getSchedule() != null && !job.getSchedule().isEmpty()) {
                resultJobs.add(job);
            }
        }

        return resultJobs;
    }

    private Map<Long, Response<String>> getAllJobsAsRedisResponse(List<Long> jobIds) {

        List<Job> resultJobs = new ArrayList<>();

        Map<Long, Response<String>> responseMap = new HashMap<>();

        long size = jedisExecution.execute(new JedisOperation<Long>() {

            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                long length = 0;
                Pipeline pipeline = jedis.pipelined();

                for (Long jobId : jobIds) {
                    String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);

                    Response<String> foundJob = pipeline.get(jobKey);
                    responseMap.put(jobId, foundJob);
                    length++;
                }

                pipeline.sync();
                return length;
            }

        });

        for (Response<String> response : responseMap.values()) {
            Job job = WorkhorseUtil.jsonToParameters(response.get(), Job.class);
            resultJobs.add(job);
        }

        return responseMap;
    }

    private Map<Long, Response<String>> getAllJobsAsRedisResponse() {

        String jobsListKey = RedisKey.LIST_OF_JOB.getQuery();
        List<Long> jobIds = redisService.lrange(jobsListKey, Long.class, 0, -1);

        return getAllJobsAsRedisResponse(jobIds);
    }

    @Override
    public JobStatusCount getJobStatusCount() {

        String activeJobKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(JobStatus.ACTIVE);
        long countActive = redisService.llen(activeJobKey);

        String inactiveJobKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(JobStatus.INACTIVE);
        long countInactive = redisService.llen(inactiveJobKey);

        String errorJobKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(JobStatus.ERROR);
        long countError = redisService.llen(errorJobKey);

        String noWorkerJobKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(JobStatus.NO_WORKER);
        long countNoWorker = redisService.llen(noWorkerJobKey);

        return new JobStatusCount(countActive, countInactive, countNoWorker, countError);
    }

    @Override
    public Long count() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long countByStatus(JobStatus jobStatus) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Job persist(Job job) {
        Long id = redisService.incr(RedisKey.INC_JOB_ID.getQuery());
        String jobKey = RedisKey.JOB_BY_ID.getQuery(id);
        String jobNameKey = RedisKey.JOB_BY_NAME.getQuery(job.getName());
        String workerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(job.getWorkerClassName());
        String jobListKey = RedisKey.LIST_OF_JOB.getQuery();
        String jobListByStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(job.getStatus());

        job.setId(id);
        job.setCreatedAt(WorkhorseUtil.timestamp());

        // add the job as JSON
        redisService.set(jobKey, job);

        // add the ID of the job in to the job's name
        redisService.set(jobNameKey, id);

        // add the ID of the job in to the workername
        redisService.set(workerNameKey, id);

        // add the ID of the job in the list of jobs
        redisService.rpush(jobListKey, id);

        // add the ID of the job to the list of job on the given status
        redisService.rpush(jobListByStatusKey, id);

        return job;
    }

    // Think about put it in a redis transaction
    @Override
    public Job update(Job newJob) {

        Long jobId = newJob.getId();

        String jobKey = RedisKey.JOB_BY_ID.getQuery(newJob.getId());

        Job oldJob = redisService.get(jobKey, Job.class);

        newJob.setUpdatedAt(WorkhorseUtil.timestamp());
        redisService.set(jobKey, newJob);

        if (!Objects.equals(oldJob.getName(), newJob.getName())) {

            String oldJobNameKey = RedisKey.JOB_BY_NAME.getQuery(oldJob.getName());
            redisService.del(oldJobNameKey);

            String newJobNameKey = RedisKey.JOB_BY_NAME.getQuery(newJob.getName());
            redisService.set(newJobNameKey, jobId);
        }

        if (!Objects.equals(oldJob.getWorkerClassName(), newJob.getWorkerClassName())) {

            String oldWorkerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(oldJob.getWorkerClassName());
            redisService.del(oldWorkerNameKey);

            String newWorkerNameKey = RedisKey.JOB_BY_WORKER_NAME.getQuery(newJob.getWorkerClassName());
            redisService.set(newWorkerNameKey, jobId);
        }

        if (!Objects.equals(oldJob.getStatus(), newJob.getStatus())) {

            String jobListByOldStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(oldJob.getStatus());
            redisService.lrem(jobListByOldStatusKey, jobId);

            String jobListByNewStatusKey = RedisKey.LIST_OF_JOB_BY_STATUS.getQuery(newJob.getStatus());
            redisService.rpush(jobListByNewStatusKey, jobId);

        }

        return redisService.get(jobKey, Job.class);

    }

    @Override
    public void deleteJob(Long jobId) {

    }

    @Override
    public void connect(Object... params) {
        return;
    }

    @Override
    public String getPersistenceName() {
        return RedisPersistenceConfig.NAME;
    }

}
