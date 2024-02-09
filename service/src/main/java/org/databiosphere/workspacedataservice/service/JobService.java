package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
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

  public List<GenericJobServerModel> getJobsForInstance(
      InstanceId instanceId, List<String> statuses) {
    return jobDao.getJobsForInstance(instanceId, statuses);
  }
}
