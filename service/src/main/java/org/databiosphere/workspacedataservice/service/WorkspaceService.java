package org.databiosphere.workspacedataservice.service;

import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkspaceService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final JobDao jobDao;
  private final SchedulerDao schedulerDao;

  public WorkspaceService(JobDao jobDao, SchedulerDao schedulerDao) {
    this.jobDao = jobDao;
    this.schedulerDao = schedulerDao;
  }

  public GenericJobServerModel initWorkspace(
      WorkspaceId workspaceId, WorkspaceInitServerModel workspaceInitServerModel) {

    logger.info("Starting init-workspace for workspaceId {}", workspaceId);

    Optional<UUID> sourceWorkspace =
        Optional.ofNullable(workspaceInitServerModel.getClone())
            .flatMap(clone -> Optional.ofNullable(clone.getSourceWorkspaceId()));

    if (sourceWorkspace.isPresent()) {
      // this is a clone.
      // TODO AJ-1952: implement cloning
    } else {
      // this is not a clone; create an empty default collection.
    }

    GenericJobServerModel job = new GenericJobServerModel();
    return job;
  }
}
