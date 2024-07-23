package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.CollectionRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WdsCollectionCreateRequest;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class CollectionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);
  private final CollectionDao collectionDao;
  private final SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  private final ActivityLogger activityLogger;
  private final TenancyProperties tenancyProperties;

  private final CollectionRepository collectionRepository;
  private final NamedParameterJdbcTemplate namedTemplate;

  @Nullable private WorkspaceId workspaceId;

  public CollectionService(
      CollectionDao collectionDao,
      SamAuthorizationDaoFactory samAuthorizationDaoFactory,
      ActivityLogger activityLogger,
      TenancyProperties tenancyProperties,
      CollectionRepository collectionRepository,
      NamedParameterJdbcTemplate namedTemplate) {
    this.collectionDao = collectionDao;
    this.samAuthorizationDaoFactory = samAuthorizationDaoFactory;
    this.activityLogger = activityLogger;
    this.tenancyProperties = tenancyProperties;
    this.collectionRepository = collectionRepository;
    this.namedTemplate = namedTemplate;
  }

  @Autowired(required = false) // control plane won't have workspaceId
  void setWorkspaceId(@Nullable @SingleTenant WorkspaceId workspaceId) {
    this.workspaceId = workspaceId;
  }

  // ============================== v1 methods ==============================

  /**
   * Insert a new collection
   *
   * @param workspaceId the workspace to contain this collection
   * @param collectionServerModel the collection definition
   * @return the created collection
   */
  public CollectionServerModel save(
      WorkspaceId workspaceId, CollectionServerModel collectionServerModel) {

    // check that the current user has permission on the parent workspace
    if (!canCreateCollection(workspaceId)) {
      // the user doesn't have permission to create a new collection.
      // do they have permission to read the workspace at all?
      if (canReadWorkspace(workspaceId)) {
        throw new AuthorizationException("Caller does not have permission to create collection.");
      } else {
        throw new AuthenticationMaskableException("Workspace");
      }
    }
    // TODO: validate name against the CollectionServerModel.getName() pattern

    // if user did not specify an id, generate one
    CollectionId collectionId;
    if (collectionServerModel.getId() != null) {
      collectionId = CollectionId.of(collectionServerModel.getId());
    } else {
      collectionId = CollectionId.of(UUID.randomUUID());
    }
    // translate CollectionServerModel to WdsCollection
    WdsCollection wdsCollectionRequest =
        new WdsCollectionCreateRequest(
            workspaceId,
            collectionId,
            collectionServerModel.getName(),
            collectionServerModel.getDescription());

    // save
    WdsCollection actual = null;
    try {
      actual = collectionRepository.save(wdsCollectionRequest);
    } catch (DbActionExecutionException dbActionExecutionException) {
      handleDbException(dbActionExecutionException);
    }

    // create the postgres schema itself
    namedTemplate.getJdbcTemplate().update("create schema " + quote(collectionId.toString()));

    activityLogger.saveEventForCurrentUser(
        user -> user.created().collection().withUuid(collectionId.id()));

    // translate back to CollectionServerModel
    CollectionServerModel response = new CollectionServerModel(actual.name(), actual.description());
    response.id(actual.collectionId().id());

    return response;
  }

  /**
   * Delete a collection.
   *
   * @param workspaceId the workspace containing the collection to be deleted
   * @param collectionId id of the collection to be deleted
   */
  public void delete(WorkspaceId workspaceId, CollectionId collectionId) {
    // validate permissions
    boolean canWriteWorkspace = canWriteWorkspace(workspaceId);
    boolean canReadWorkspace = canReadWorkspace(workspaceId);

    // check that the current user has permission to delete the collection
    if (!canWriteWorkspace) {
      // the user doesn't have permission to delete the collection.
      // do they have permission to list collections in the workspace?
      if (canReadWorkspace) {
        throw new AuthorizationException(
            "Caller does not have permission to delete collections from this workspace.");
      } else {
        throw new AuthenticationMaskableException("Workspace");
      }
    }

    // user is permitted to delete collections (can write the workspace); now, validate the
    // collection
    WorkspaceId collectionWorkspace = getWorkspaceId(collectionId);
    // ensure this collection belongs to this workspace
    if (!collectionWorkspace.equals(workspaceId)) {
      throw new ValidationException("Collection does not belong to the specified workspace");
    }

    // delete the schema from Postgres
    namedTemplate
        .getJdbcTemplate()
        .update("drop schema " + quote(collectionId.toString()) + " cascade");

    collectionRepository.deleteById(collectionId.id());

    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().collection().withUuid(collectionId.id()));
  }

  /**
   * List all collections in a given workspace. Does not paginate.
   *
   * @param workspaceId the workspace in which to list collections.
   * @return all collections in the given workspace
   */
  public List<CollectionServerModel> list(WorkspaceId workspaceId) {
    if (!canListCollections(workspaceId)) {
      throw new AuthenticationMaskableException("Workspace");
    }

    Iterable<WdsCollection> found = collectionRepository.findByWorkspace(workspaceId);
    // map the WdsCollection to CollectionServerModel
    Stream<WdsCollection> colls =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(found.iterator(), Spliterator.ORDERED), false);

    return colls
        .map(
            wdsCollection -> {
              CollectionServerModel serverModel =
                  new CollectionServerModel(wdsCollection.name(), wdsCollection.description());
              serverModel.id(wdsCollection.collectionId().id());
              return serverModel;
            })
        .toList();
  }

  // exception handling
  private void handleDbException(DbActionExecutionException dbActionExecutionException) {
    Throwable cause = dbActionExecutionException.getCause();
    if (cause == null) {
      throw dbActionExecutionException;
    }
    if (cause instanceof DuplicateKeyException duplicateKeyException) {
      // kinda ugly: we need to determine if the DuplicateKeyException is due to an id conflict
      // or a name conflict.
      String msg = duplicateKeyException.getMessage();
      if (msg.contains("duplicate key value violates unique constraint")
          && msg.contains("instance_pkey")) {
        // TODO: this allows phishing for collection ids.
        throw new ConflictException("Collection with this id already exists");
      } else if (msg.contains("duplicate key value violates unique constraint")
          && msg.contains("instance_workspace_id_name_key")) {
        throw new ConflictException("Collection with this name already exists in this workspace");
      }
      throw new ConflictException("Collection name or id conflict");
    }
    throw dbActionExecutionException;
  }

  // ============================== v0.2 methods ==============================

  public List<UUID> listCollections(String version) {
    validateVersion(version);
    return collectionDao.listCollectionSchemas().stream().map(CollectionId::id).toList();
  }

  /**
   * @deprecated Use {@link #createCollection(WorkspaceId, CollectionId, String)}
   */
  @Deprecated
  public void createCollection(UUID collectionId, String version) {
    if (tenancyProperties.getAllowVirtualCollections()) {
      throw new CollectionException(
          "createCollection not allowed when virtual collections are enabled");
    }
    if (workspaceId == null) {
      throw new CollectionException(
          "createCollection requires a workspaceId to be configured or provided");
    }
    createCollection(workspaceId, CollectionId.of(collectionId), version);
  }

  /**
   * Creates a WDS collection, comprised of a Postgres schema and a Sam resource of type
   * "workspace". The Postgres schema will have an id of `collectionId`, which will match the Sam
   * resource's`workspaceId` unless otherwise specified.
   *
   * <p>TODO(AJ-1662): wire workspaceId down through collectionDao
   *
   * @param collectionId id of the collection to create
   * @param version WDS API version
   */
  public void createCollection(WorkspaceId workspaceId, CollectionId collectionId, String version) {
    validateVersion(version);

    // check that the current user has permission on the parent workspace
    if (!canCreateCollection(workspaceId)) {
      throw new AuthorizationException("Caller does not have permission to create collection.");
    }

    if (collectionDao.collectionSchemaExists(collectionId)) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "This collection already exists");
    }

    // TODO(AJ-1662): this needs to pass the workspaceId argument so the collection is created
    //   correctly
    // create collection schema in Postgres
    collectionDao.createSchema(collectionId);

    activityLogger.saveEventForCurrentUser(
        user -> user.created().collection().withUuid(collectionId.id()));
  }

  public void deleteCollection(UUID collectionUuid, String version) {
    validateVersion(version);
    validateCollection(collectionUuid);
    CollectionId collectionId = CollectionId.of(collectionUuid);

    // check that the current user has permission to delete the Sam resource
    if (!canDeleteCollection(collectionId)) {
      throw new AuthorizationException("Caller does not have permission to delete collection.");
    }

    // delete collection schema in Postgres
    collectionDao.dropSchema(collectionId);

    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().collection().withUuid(collectionUuid));
  }

  public void validateCollection(UUID collectionId) {
    // if this deployment allows virtual collections, there is nothing to validate
    if (tenancyProperties.getAllowVirtualCollections()) {
      return;
    }
    // else, check if this collection has a row in the collections table
    if (!collectionDao.collectionSchemaExists(CollectionId.of(collectionId))) {
      throw new MissingObjectException("Collection");
    }
  }

  public boolean canListCollections(WorkspaceId workspaceId) {
    return canReadWorkspace(workspaceId);
  }

  public boolean canReadCollection(CollectionId collectionId) {
    return canReadWorkspace(getWorkspaceId(collectionId));
  }

  public boolean canWriteCollection(CollectionId collectionId) {
    return canWriteWorkspace(getWorkspaceId(collectionId));
  }

  private boolean canCreateCollection(WorkspaceId workspaceId) {
    return canWriteWorkspace(workspaceId);
  }

  private boolean canDeleteCollection(CollectionId collectionId) {
    return canWriteWorkspace(getWorkspaceId(collectionId));
  }

  private boolean canReadWorkspace(WorkspaceId workspaceId) {
    return getSamAuthorizationDao(workspaceId).hasReadWorkspacePermission();
  }

  private boolean canWriteWorkspace(WorkspaceId workspaceId) {
    return getSamAuthorizationDao(workspaceId).hasWriteWorkspacePermission();
  }

  /**
   * Return the workspace that contains the specified collection. The logic for this method is:
   *
   * <p>- look up the row for this collection in sys_wds.collection.
   *
   * <p>- if the WORKSPACE_ID env var is set and a row exists in sys_wds.collection, confirm the
   * row's workspace_id column matches the env var. If it does, return its value. If it doesn't
   * match, throw an error.
   *
   * <p>- else, return the CollectionId value as the WorkspaceId. This perpetuates the assumption
   * that collection and workspace ids are the same. But, this will be true in the GCP control plane
   * as long as Rawls continues to manage data tables, since "collection" is a WDS concept and does
   * not exist in Rawls.
   *
   * @param collectionId the collection for which to look up the workspace
   * @return the workspace containing the given collection.
   */
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    // look up the workspaceId for this collection in the collection table
    WorkspaceId rowWorkspaceId = null;
    try {
      rowWorkspaceId = collectionDao.getWorkspaceId(collectionId);
      // as of this writing, if a deployment allows virtual collections, it is an error if the
      // collection DOES exist.
      if (tenancyProperties.getAllowVirtualCollections()) {
        throw new CollectionException("Expected a virtual collection");
      }
    } catch (EmptyResultDataAccessException e) {
      if (!tenancyProperties.getAllowVirtualCollections()) {
        throw new MissingObjectException("Collection");
      }
    }

    // safety check: if we found a workspace id in the db, it indicates we are in a data-plane
    // single-tenant WDS. Verify the workspace matches the $WORKSPACE_ID env var.
    // we must remove this check in a future multi-tenant WDS.
    if (tenancyProperties.getEnforceCollectionsMatchWorkspaceId()
        && rowWorkspaceId != null
        && !rowWorkspaceId.equals(workspaceId)) {
      // log the details, including expected/actual workspace ids
      LOGGER.error(
          "Found unexpected workspaceId for collection {}. Expected {}, got {}.",
          collectionId,
          workspaceId,
          rowWorkspaceId);
      // but, don't include workspace ids when throwing a user-facing error
      throw new CollectionException(
          "Found unexpected workspaceId for collection %s.".formatted(collectionId));
    }
    return Objects.requireNonNullElseGet(rowWorkspaceId, () -> WorkspaceId.of(collectionId.id()));
  }

  private SamAuthorizationDao getSamAuthorizationDao(WorkspaceId workspaceId) {
    return samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId);
  }
}
