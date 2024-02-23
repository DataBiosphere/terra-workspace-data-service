package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.InstanceProperties.SingleTenant;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceIdDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class CollectionService {

  @Nullable private CollectionDao collectionDao;
  private final WorkspaceIdDao workspaceIdDao;
  private final SamDao samDao;
  private final ActivityLogger activityLogger;
  private final TenancyProperties tenancyProperties;

  @Nullable private WorkspaceId environmentWorkspaceId;

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);

  public CollectionService(
      WorkspaceIdDao workspaceIdDao,
      SamDao samDao,
      ActivityLogger activityLogger,
      TenancyProperties tenancyProperties) {
    this.workspaceIdDao = workspaceIdDao;
    this.samDao = samDao;
    this.activityLogger = activityLogger;
    this.tenancyProperties = tenancyProperties;
  }

  @Autowired(required = false) // not provided in control-plane deployments
  void setCollectionDao(CollectionDao collectionDao) {
    this.collectionDao = collectionDao;
  }

  // instanceProperties only required when tenancyProperties.getAllowVirtualCollections() is false
  // TODO(AJ-1655): make instanceProperties an Optional child of tenancyProperties, present only
  //   when allow-virtual-collections=false?
  @Autowired(required = false)
  public void setWorkspaceId(@SingleTenant WorkspaceId workspaceId) {
    this.environmentWorkspaceId = workspaceId;
  }

  public List<UUID> listCollections(String version) {
    validateVersion(version);
    return collectionDao.listCollectionSchemas();
  }

  // TODO update this javadoc, also are there logical problems with the sam resource type?
  // Also check the naming on these methods here
  /**
   * Creates a WDS collection, comprised of a Postgres schema and a Sam resource of type
   * "workspace". The Postgres schema will have an id of `collectionId`, which will match the Sam
   * resource's`workspaceId` unless otherwise specified.
   *
   * @param collectionId id of the collection to create
   * @param version WDS API version
   */
  public void createCollection(UUID collectionId, String version) {
    validateVersion(version);

    // check that the current user has permission on the parent workspace
    boolean hasCreateCollectionPermission = samDao.hasCreateCollectionPermission();
    LOGGER.debug("hasCreateCollectionPermission? {}", hasCreateCollectionPermission);

    if (!hasCreateCollectionPermission) {
      throw new AuthorizationException("Caller does not have permission to create collection.");
    }

    if (collectionDao.collectionSchemaExists(collectionId)) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "This collection already exists");
    }

    // create collection schema in Postgres
    collectionDao.createSchema(collectionId);

    activityLogger.saveEventForCurrentUser(
        user -> user.created().collection().withUuid(collectionId));
  }

  public void deleteCollection(UUID collectionId, String version) {
    validateVersion(version);
    validateCollection(collectionId);

    // check that the current user has permission to delete the Sam resource
    boolean hasDeleteCollectionPermission = samDao.hasDeleteCollectionPermission();
    LOGGER.debug("hasDeleteCollectionPermission? {}", hasDeleteCollectionPermission);

    if (!hasDeleteCollectionPermission) {
      throw new AuthorizationException("Caller does not have permission to delete collection.");
    }

    // delete collection schema in Postgres
    collectionDao.dropSchema(collectionId);

    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().collection().withUuid(collectionId));
  }

  public void validateCollection(UUID collectionId) {
    // if this deployment allows virtual collections, there is nothing to validate
    if (tenancyProperties.getAllowVirtualCollections()) {
      return;
    }
    // else, check if this collection has a row in the collections table
    if (!collectionDao.collectionSchemaExists(collectionId)) {
      throw new MissingObjectException("Collection");
    }
  }

  public boolean canReadCollection(CollectionId collectionId) {
    try {
      WorkspaceId workspaceId = getWorkspaceId(collectionId);
      return samDao.hasReadWorkspacePermission(workspaceId.toString());
    } catch (MissingObjectException e) {
      return false;
    }
  }

  public boolean canWriteCollection(CollectionId collectionId) {
    try {
      WorkspaceId workspaceId = getWorkspaceId(collectionId);
      return samDao.hasWriteWorkspacePermission(workspaceId.toString());
    } catch (MissingObjectException e) {
      return false;
    }
  }

  /**
   * Return the {@link WorkspaceId} of the workspace that contains the specified collection. The
   * logic for this method is:
   *
   * <p>- if the WORKSPACE_ID env var is set, look up the row for this collection in
   * sys_wds.collection
   *
   * <p>- confirm resulting workspace_id matches the env var. If it does, return its value. If it
   * doesn't match, throw a {@link CollectionException}.
   *
   * <p>- else, return the CollectionId value as the WorkspaceId. This perpetuates the assumption
   * that collection and workspace ids are the same. But, this will be true in the GCP control plane
   * as long as Rawls continues to manage data tables, since "collection" is a WDS concept and does
   * not exist in Rawls.
   *
   * @param collectionId the collection for which to look up the workspace
   * @return the {@link WorkspaceId} of the workspace containing the given collection
   * @throws MissingObjectException when the collection does not exist and the environment does not
   *     allow virtual collections
   * @throws CollectionException when the persisted workspaceId does not match the environment
   */
  public WorkspaceId getWorkspaceId(CollectionId collectionId) throws MissingObjectException {
    // look up the workspaceId for this collection in the collection table; in the case of a virtual
    // collection, this will just echo the same value as what was passed in
    WorkspaceId workspaceId = workspaceIdDao.getWorkspaceId(collectionId);

    // if single-tenant WDS, verify the workspace matches the $WORKSPACE_ID env var
    if (isSingleTenantWds() && !workspaceId.equals(environmentWorkspaceId)) {
      // log the details, including expected/actual workspace ids
      LOGGER.error(
          "Found unexpected workspaceId for collection {}. Expected {}, got {}.",
          collectionId,
          environmentWorkspaceId,
          workspaceId);
      // but, don't include workspace ids when throwing a user-facing error
      throw new CollectionException(
          "Found unexpected workspaceId for collection %s.".formatted(collectionId));
    }

    return workspaceId;
  }

  private boolean isSingleTenantWds() {
    return environmentWorkspaceId != null;
  }
}
