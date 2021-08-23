package io.coodoo.workhorse.persistence.redis.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.Job;

@ApplicationScoped
public class JobsCache {

    private Map<Long, Job> jobs = new ConcurrentHashMap<>();

    @Inject
    RedisClient redisService;

    public List<Job> getJobs() {
        List<Job> jobList = new ArrayList<>();

        // Set<Long> jobIds = jobs.keySet();
        // if (jobIds.isEmpty()) {
        // jobList = getAll();
        // for (Job job : jobList) {
        // jobs.put(job.getId(), job);
        // }
        // }
        //
        // for (Long jobId : jobs.keySet()) {
        //
        // jobList.add(jobs.get(jobId));
        // }

        return jobList;
    }

}
