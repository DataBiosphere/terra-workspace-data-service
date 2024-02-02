package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class InstanceService {

  private final InstanceDao instanceDao;
  private final SamDao samDao;
  private final ActivityLogger activityLogger;

  private final WorkspaceId workspaceId;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceService.class);

  public InstanceService(
      InstanceDao instanceDao,
      SamDao samDao,
      ActivityLogger activityLogger,
      @Value("${twds.instance.workspace-id:}") String workspaceIdProperty) {
    this.instanceDao = instanceDao;
    this.samDao = samDao;
    this.activityLogger = activityLogger;
    // jump through a few hoops to initialize workspaceId to ensure validity.
    // workspaceId will be null if the WORKSPACE_ID env var is unset/not a valid UUID.
    UUID workspaceUuid = null;
    try {
      workspaceUuid = UUID.fromString(workspaceIdProperty);
    } catch (Exception e) {
      LOGGER.warn("WORKSPACE_ID could not be parsed into a UUID: {}", workspaceIdProperty);
    }
    if (workspaceUuid == null) {
      workspaceId = null;
    } else {
      workspaceId = WorkspaceId.of(workspaceUuid);
    }
  }

  public List<UUID> listInstances(String version) {
    validateVersion(version);
    return instanceDao.listInstanceSchemas();
  }

  /**
   * Creates a WDS instance, comprised of a Postgres schema and a Sam resource of type
   * "wds-instance". The Postgres schema and Sam resource will both have an id of `instanceId`. The
   * Sam resource will have a parent of type `workspace`. The parent's id will be `workspaceId` if
   * specified, else will be `instanceId`. In the latter case where a `workspaceId` is not
   * specified, this will result in two Sam resources having the same id - one of type `workspace`
   * and another of type `wds-instance.
   *
   * @param instanceId id of the instance to create
   * @param version WDS API version
   */
  public void createInstance(UUID instanceId, String version) {
    validateVersion(version);

    // check that the current user has permission on the parent workspace
    boolean hasCreateInstancePermission = samDao.hasCreateInstancePermission();
    LOGGER.debug("hasCreateInstancePermission? {}", hasCreateInstancePermission);

    if (!hasCreateInstancePermission) {
      throw new AuthorizationException("Caller does not have permission to create instance.");
    }

    if (instanceDao.instanceSchemaExists(instanceId)) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "This instance already exists");
    }

    // create instance schema in Postgres
    instanceDao.createSchema(instanceId);

    activityLogger.saveEventForCurrentUser(user -> user.created().instance().withUuid(instanceId));
  }

  public void deleteInstance(UUID instanceId, String version) {
    validateVersion(version);
    validateInstance(instanceId);

    // check that the current user has permission to delete the Sam resource
    boolean hasDeleteInstancePermission = samDao.hasDeleteInstancePermission();
    LOGGER.debug("hasDeleteInstancePermission? {}", hasDeleteInstancePermission);

    if (!hasDeleteInstancePermission) {
      throw new AuthorizationException("Caller does not have permission to delete instance.");
    }

    // delete instance schema in Postgres
    instanceDao.dropSchema(instanceId);

    activityLogger.saveEventForCurrentUser(user -> user.deleted().instance().withUuid(instanceId));
  }

  public void validateInstance(UUID instanceId) {
    if (!instanceDao.instanceSchemaExists(instanceId)) {
      throw new MissingObjectException("Instance");
    }
  }

  /**
   * Return the workspace that contains the specified instance. The logic for this method is:
   *
   * <p>- if the WORKSPACE_ID env var is set, return its value. This will be true for all data plane
   * WDSes, as long as WDS is deployed per-workspace.
   *
   * <p>- else, return the InstanceId value as the WorkspaceId. This perpetuates the assumption that
   * instance and workspace ids are the same. But, this will be true in the GCP control plane as
   * long as Rawls continues to manage data tables, since "instance" is a WDS concept and does not
   * exist in Rawls.
   *
   * @param instanceId the instance for which to look up the workspace
   * @return the workspace containing the given instance.
   */
  public WorkspaceId getWorkspaceId(InstanceId instanceId) {
    if (workspaceId == null) {
      return WorkspaceId.of(instanceId.id());
    }
    return workspaceId;
  }
}
