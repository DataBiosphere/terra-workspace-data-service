package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  JobDao jobDao;
  InstanceService instanceService;
  SamDao samDao;

  public JobService(JobDao jobDao, InstanceService instanceService, SamDao samDao) {
    this.jobDao = jobDao;
    this.instanceService = instanceService;
    this.samDao = samDao;
  }

  public GenericJobServerModel getJob(UUID jobId) {
    try {
      GenericJobServerModel result = jobDao.getJob(jobId);
      if (!samDao.hasReadInstancePermission(result.getInstanceId().toString())) {
        throw new AuthorizationException("Caller does not have permission to view this job.");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForInstance(
      InstanceId instanceId, List<String> statuses) {
    if (!samDao.hasReadInstancePermission(instanceId.toString())) {
      throw new AuthorizationException("Caller does not have permission to view this job.");
    }
    return jobDao.getJobsForInstance(instanceId, statuses);
  }
}
