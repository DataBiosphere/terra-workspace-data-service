package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.model.InstanceId;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  JobDao jobDao;
  InstanceService instanceService;

  public JobService(JobDao jobDao, InstanceService instanceService) {
    this.jobDao = jobDao;
    this.instanceService = instanceService;
  }

  public GenericJobServerModel getJob(UUID jobId) {
    try {
      return jobDao.getJob(jobId);
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> listJobs(InstanceId instanceId) {
    return jobDao.listJobs(instanceId);
  }

  public GenericJobServerModel getJob(InstanceId instanceId, UUID jobId) {
    return jobDao.getJob(instanceId, jobId);
  }
}
