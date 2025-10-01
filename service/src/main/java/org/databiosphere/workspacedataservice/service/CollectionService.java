package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionRepository;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.rawls.RawlsException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.DefaultCollectionCreationResult;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class CollectionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);

  private final ActivityLogger activityLogger;
  private final CollectionRepository collectionRepository;
  private final DataTableTypeInspector dataTableTypeInspector;
  private final NamedParameterJdbcTemplate namedTemplate;
  private final TenancyProperties tenancyProperties;
  private final TwdsProperties twdsProperties;

  // strings for use in error messages
  private static final String COLLECTION = "Collection";
  public static final String NAME_DEFAULT = "default";

  public CollectionService(
      ActivityLogger activityLogger,
      CollectionRepository collectionRepository,
      DataTableTypeInspector dataTableTypeInspector,
      NamedParameterJdbcTemplate namedTemplate,
      TenancyProperties tenancyProperties,
      TwdsProperties twdsProperties) {
    this.activityLogger = activityLogger;
    this.collectionRepository = collectionRepository;
    this.dataTableTypeInspector = dataTableTypeInspector;
    this.namedTemplate = namedTemplate;
    this.tenancyProperties = tenancyProperties;
    this.twdsProperties = twdsProperties;
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
   * Convenience method to insert a new collection of a given name and description
   *
   * @see #save(WorkspaceId, CollectionRequestServerModel)
   * @param workspaceId the workspace to contain this collection
   * @param name name for the created collection
   * @param description description for the created collection
   * @return the created collection
   */
  @WriteTransaction
  public CollectionServerModel save(WorkspaceId workspaceId, String name, String description) {
    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);
    return save(workspaceId, collectionRequestServerModel);
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
        && !workspaceId.equals(twdsProperties.workspaceId())) {
      throw new ValidationException("Cannot create collection in this workspace.");
    }

    // translate CollectionServerModel to WdsCollection
    WdsCollection wdsCollectionRequest =
        new WdsCollection(
            workspaceId,
            collectionId,
            collectionRequestServerModel.getName(),
            collectionRequestServerModel.getDescription(),
            /* newFlag= */ true);

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

    collectionRepository.deleteById(collectionId);

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
   * Does a collection exist at the given id?
   *
   * @param collectionId the collection id to inspect
   * @return whether the collection exists
   */
  public boolean exists(CollectionId collectionId) {
    return collectionRepository.existsById(collectionId);
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
  @WriteTransaction
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

  // ============================== helpers ==============================

  /**
   * Return the workspace that contains the specified collection, validating against the
   * twds.tenancy.enforce-collections-match-workspace-id configuration setting.
   *
   * <p>For WDS-powered workspaces, returns the value from the sys_wds.collection table, or throws
   * an error if a row does not exist in that table.
   *
   * <p>For RAWLS-powered workspaces, if twds.tenancy.allow-virtual-collections is true, echoes back
   * the input collection id as the workspace id; this is a virtual collection. If
   * twds.tenancy.allow-virtual-collections is false, throws an error.
   *
   * @see DataTableTypeInspector for logic on determining if a workspace is WDS-powered or
   *     RAWLS-powered.
   * @param collectionId the collection for which to look up the workspace
   * @return the workspace containing the given collection.
   */
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    WorkspaceId workspaceId = calculateWorkspaceId(collectionId);

    // safety check: if this is a single-tenant WDS, verify that the workspace matches the
    // $WORKSPACE_ID env var.
    if (tenancyProperties.getEnforceCollectionsMatchWorkspaceId()
        && !workspaceId.equals(twdsProperties.workspaceId())) {
      // log the details, including expected/actual workspace ids
      LOGGER.error(
          "Found unexpected workspaceId for collection {}. Expected {}, got {}.",
          collectionId,
          twdsProperties.workspaceId(),
          workspaceId);
      // but, don't include workspace ids when throwing a user-facing error
      throw new CollectionException(
          "Found unexpected workspaceId for collection %s.".formatted(collectionId));
    }
    return workspaceId;
  }

  /**
   * Determines the workspaceId for a given collection; does not validate against
   * twds.tenancy.enforce-collections-match-workspace-id
   *
   * @param collectionId the collection for which to look up the workspace
   * @return the workspace containing the given collection.
   */
  private WorkspaceId calculateWorkspaceId(CollectionId collectionId) {
    // look up the workspaceId for this collection in the collection table
    Optional<WdsCollection> maybeCollection = collectionRepository.findById(collectionId);

    // row exists; this is the ideal case
    if (maybeCollection.isPresent()) {
      return maybeCollection.get().workspaceId();
    }

    // row does not exist. handle the possibility that this is a virtual collection.

    // if virtual collections are not allowed, this is an error.
    if (!tenancyProperties.getAllowVirtualCollections()) {
      throw new MissingObjectException(COLLECTION);
    }

    // if this is a virtual collection, its workspace id will be equal to the collection id
    WorkspaceId virtualWorkspaceId = WorkspaceId.of(collectionId.id());

    // ask if this is a known workspace
    try {
      return switch (dataTableTypeInspector.getWorkspaceDataTableType(virtualWorkspaceId)) {
        case RAWLS -> // this is a known, Rawls-powered workspace. It's a virtual collection.
            virtualWorkspaceId;
        case WDS -> // this is a known, WDS-powered workspace. Virtual collections are not allowed
            // for WDS-powered workspaces. This is an error.
            throw new MissingObjectException(COLLECTION);
      };
    } catch (RawlsException rawlsException) {
      if (HttpStatus.NOT_FOUND.equals(rawlsException.getStatusCode())) {
        // the workspace is unknown; this is an error
        throw new MissingObjectException(COLLECTION);
      }
      // some other error occurred when checking the workspace
      throw new CollectionException("Unexpected error validating collection");
    }
  }
}
