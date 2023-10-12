package org.databiosphere.workspacedataservice.service;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  JobDao jobDao;
  InstanceService instanceService;

  public JobService(JobDao jobDao, InstanceService instanceService) {
    this.jobDao = jobDao;
    this.instanceService = instanceService;
  }

  public GenericJobServerModel getJob(UUID instanceUuid, UUID jobId) {
    // validate instance exists
    instanceService.validateInstance(instanceUuid);

    return jobDao.getJob(jobId);
  }
}
