package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.DefaultCollectionCreationResult;
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
  private final ActivityLogger activityLogger;
  private final TenancyProperties tenancyProperties;

  private final CollectionRepository collectionRepository;
  private final NamedParameterJdbcTemplate namedTemplate;

  // strings for use in error messages
  private static final String COLLECTION = "Collection";
  public static final String NAME_DEFAULT = "default";

  @Nullable private WorkspaceId workspaceId;

  public CollectionService(
      CollectionDao collectionDao,
      ActivityLogger activityLogger,
      TenancyProperties tenancyProperties,
      CollectionRepository collectionRepository,
      NamedParameterJdbcTemplate namedTemplate) {
    this.collectionDao = collectionDao;
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

  @WriteTransaction
  public DefaultCollectionCreationResult createDefaultCollection(WorkspaceId workspaceId) {
    // the default collection for any workspace has the same id as the workspace
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // does the default collection already exist?
    Optional<CollectionServerModel> found = find(workspaceId, collectionId);

    // if collection exists, this is a noop; return what we found.
    if (found.isPresent()) {
      LOGGER.debug(
          "createDefaultCollection called for workspaceId {}, but workspace already has a default collection.",
          workspaceId);
      return new DefaultCollectionCreationResult(false, found.get());
    }

    // initialize the name/description payload
    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(NAME_DEFAULT, NAME_DEFAULT);
    // and save
    return new DefaultCollectionCreationResult(
        true, save(workspaceId, collectionId, collectionRequestServerModel));
  }

  /**
   * Insert a new collection, generating a random collection id.
   *
   * @param workspaceId the workspace to contain this collection
   * @param collectionRequestServerModel the collection definition
   * @return the created collection
   */
  @WriteTransaction
  public CollectionServerModel save(
      WorkspaceId workspaceId, CollectionRequestServerModel collectionRequestServerModel) {
    // generate a collection id
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    return save(workspaceId, collectionId, collectionRequestServerModel);
  }

  /**
   * Insert a new collection, specifying the collection id. This should only be called internally;
   * users should not be granted the ability to specify the collection id.
   *
   * @param workspaceId the workspace to contain this collection
   * @param collectionId the id of the collection to create
   * @param collectionRequestServerModel the collection definition
   * @return the created collection
   */
  private CollectionServerModel save(
      WorkspaceId workspaceId,
      CollectionId collectionId,
      CollectionRequestServerModel collectionRequestServerModel) {

    // if WDS is running in single-tenant mode, ensure the specified workspace matches
    if (tenancyProperties.getEnforceCollectionsMatchWorkspaceId()
        && !workspaceId.equals(this.workspaceId)) {
      throw new ValidationException("Cannot create collection in this workspace.");
    }

    // translate CollectionServerModel to WdsCollection
    WdsCollection wdsCollectionRequest =
        new WdsCollectionCreateRequest(
            workspaceId,
            collectionId,
            collectionRequestServerModel.getName(),
            collectionRequestServerModel.getDescription());

    // save, handle exceptions, and translate to the response model
    CollectionServerModel response = saveAndHandleExceptions(wdsCollectionRequest);

    // create the postgres schema itself
    namedTemplate.getJdbcTemplate().update("create schema " + quote(collectionId.toString()));

    activityLogger.saveEventForCurrentUser(
        user -> user.created().collection().withUuid(collectionId.id()));

    return response;
  }

  /**
   * Delete a collection.
   *
   * @param workspaceId the workspace containing the collection to be deleted
   * @param collectionId id of the collection to be deleted
   */
  @WriteTransaction
  public void delete(WorkspaceId workspaceId, CollectionId collectionId) {
    // validate the collection
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

  /**
   * Retrieve a single collection by its workspaceId and collectionId, or empty Optional if not
   * found.
   *
   * @param workspaceId the workspace containing the collection to be retrieved
   * @param collectionId id of the collection to be retrieved
   * @return the collection, or empty Optional if not found
   */
  public Optional<CollectionServerModel> find(WorkspaceId workspaceId, CollectionId collectionId) {
    return collectionRepository
        .find(workspaceId, collectionId)
        .map(
            coll -> {
              CollectionServerModel serverModel =
                  new CollectionServerModel(coll.name(), coll.description());
              serverModel.id(coll.collectionId().id());
              return serverModel;
            });
  }

  /**
   * Retrieve a single collection by its workspaceId and collectionId. Throws MissingObjectException
   * if not found.
   *
   * @param workspaceId the workspace containing the collection to be retrieved
   * @param collectionId id of the collection to be retrieved
   * @return the collection
   */
  public CollectionServerModel get(WorkspaceId workspaceId, CollectionId collectionId) {
    Optional<CollectionServerModel> found = find(workspaceId, collectionId);
    return found.orElseThrow(() -> new MissingObjectException(COLLECTION));
  }

  /**
   * Updates the name and/or description for a collection. Updates to the collection's id or
   * workspaceId are not allowed.
   *
   * @param workspaceId the workspace containing the collection to be updated
   * @param collectionId id of the collection to be updated
   * @param collectionRequestServerModel object containing the updated name and description.
   * @return the updated collection
   */
  public CollectionServerModel update(
      WorkspaceId workspaceId,
      CollectionId collectionId,
      CollectionRequestServerModel collectionRequestServerModel) {

    // retrieve the collection; throw if collection not found
    WdsCollection found =
        collectionRepository
            .find(workspaceId, collectionId)
            .orElseThrow(() -> new MissingObjectException(COLLECTION));

    // optimization: if the request doesn't change anything, don't bother writing to the db
    if (found.name().equals(collectionRequestServerModel.getName())
        && found.description().equals(collectionRequestServerModel.getDescription())) {
      // make sure this response has an id
      CollectionServerModel response = new CollectionServerModel(found.name(), found.description());
      response.id(found.collectionId().id());
      return response;
    }

    // update the found object with the supplied name and description
    WdsCollection updateRequest =
        new WdsCollection(
            found.workspaceId(),
            found.collectionId(),
            collectionRequestServerModel.getName(),
            collectionRequestServerModel.getDescription());

    // save, handle exceptions, and translate to the response model
    CollectionServerModel response = saveAndHandleExceptions(updateRequest);

    activityLogger.saveEventForCurrentUser(
        user -> user.updated().collection().withUuid(collectionId.id()));

    // respond
    return response;
  }

  // common logic for save() and update()
  private CollectionServerModel saveAndHandleExceptions(WdsCollection saveRequest) {
    // save
    WdsCollection actual = null;
    try {
      actual = collectionRepository.save(saveRequest);
    } catch (DbActionExecutionException dbActionExecutionException) {
      handleDbException(dbActionExecutionException);
    }

    // translate to the response model
    CollectionServerModel serverModel =
        new CollectionServerModel(actual.name(), actual.description());
    serverModel.id(actual.collectionId().id());

    // and return
    return serverModel;
  }

  // exception handling for save() and update()
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

  /**
   * @deprecated Use {@link #list(WorkspaceId)} instead.
   */
  @Deprecated(forRemoval = true, since = "v0.14.0")
  public List<UUID> listCollections(String version) {
    validateVersion(version);
    return collectionDao.listCollectionSchemas().stream().map(CollectionId::id).toList();
  }

  /**
   * @deprecated Use {@link #save(WorkspaceId, CollectionRequestServerModel)} instead.
   */
  @Deprecated(forRemoval = true, since = "v0.14.0")
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
   * @deprecated Use {@link #save(WorkspaceId, CollectionRequestServerModel)} instead.
   */
  @Deprecated(forRemoval = true, since = "v0.14.0")
  public void createCollection(WorkspaceId workspaceId, CollectionId collectionId, String version) {
    validateVersion(version);

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

  /**
   * @deprecated Use {@link #delete(WorkspaceId, CollectionId)} instead.
   */
  @Deprecated(forRemoval = true, since = "v0.14.0")
  public void deleteCollection(UUID collectionUuid, String version) {
    validateVersion(version);
    validateCollection(collectionUuid);
    CollectionId collectionId = CollectionId.of(collectionUuid);

    // delete collection schema in Postgres
    collectionDao.dropSchema(collectionId);

    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().collection().withUuid(collectionUuid));
  }

  // ============================== helpers ==============================

  public void validateCollection(UUID collectionId) {
    // if this deployment allows virtual collections, there is nothing to validate
    if (tenancyProperties.getAllowVirtualCollections()) {
      return;
    }
    // else, check if this collection has a row in the collections table
    if (!collectionDao.collectionSchemaExists(CollectionId.of(collectionId))) {
      throw new MissingObjectException(COLLECTION);
    }
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
        throw new MissingObjectException(COLLECTION);
      }
    }

    // safety check: if we are running as a single-tenant WDS, verify the workspace matches the
    // $WORKSPACE_ID env var.
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
}
