package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskedException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  JobDao jobDao;
  CollectionService collectionService;

  public JobService(JobDao jobDao, CollectionService collectionService) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
  }

  public GenericJobServerModel getJob(UUID jobId) {
    try {
      GenericJobServerModel result = jobDao.getJob(jobId);
      if (!collectionService.canReadCollection(CollectionId.of(result.getInstanceId()))) {
        throw new AuthenticationMaskedException("Job");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, List<String> statuses) {
    // verify collection exists
    collectionService.validateCollection(collectionId.id());
    // check permissions
    if (!collectionService.canReadCollection(collectionId)) {
      throw new AuthenticationMaskedException("Collection");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }
}
