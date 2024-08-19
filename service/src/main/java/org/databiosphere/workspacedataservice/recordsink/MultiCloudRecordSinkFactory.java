package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.WorkspaceService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.springframework.stereotype.Component;

/** Factory to generate the appropriate {@link RecordSink} for any given workspace. */
@Component
public class MultiCloudRecordSinkFactory implements RecordSinkFactory {

  private final CollectionService collectionService;
  private final DataTypeInferer dataTypeInferer;
  private final GcsStorage storage;
  private final ObjectMapper mapper;
  private final PubSub pubSub;
  private final RecordDao recordDao;
  private final RecordService recordService;
  private final WorkspaceService workspaceService;

  public MultiCloudRecordSinkFactory(
      CollectionService collectionService,
      DataTypeInferer dataTypeInferer,
      GcsStorage storage,
      ObjectMapper mapper,
      PubSub pubSub,
      RecordDao recordDao,
      RecordService recordService,
      WorkspaceService workspaceService) {
    this.collectionService = collectionService;
    this.dataTypeInferer = dataTypeInferer;
    this.storage = storage;
    this.mapper = mapper;
    this.pubSub = pubSub;
    this.recordDao = recordDao;
    this.recordService = recordService;
    this.workspaceService = workspaceService;
  }

  /**
   * Get the {@link RecordSink} for the workspace mentioned in an import request.
   *
   * @param importDetails the import request to inspect
   * @return the appropriate {@link RecordSink} for this workspace
   */
  @Override
  public RecordSink buildRecordSink(ImportDetails importDetails) {
    return switch (workspaceService.getDataTableType(importDetails.workspaceId())) {
      case RAWLS -> RawlsRecordSink.create(mapper, storage, pubSub, importDetails);
      case WDS -> new WdsRecordSink(
          recordService, recordDao, dataTypeInferer, importDetails.collectionId());
    };
  }

  /**
   * Get the {@link RecordSink} for the workspace associated with a given collection.
   *
   * @param collectionId the collection in question
   * @return the appropriate {@link RecordSink} for this workspace
   */
  @Override
  public RecordSink buildRecordSink(CollectionId collectionId) {
    WorkspaceId workspaceId = collectionService.getWorkspaceId(collectionId);
    return switch (workspaceService.getDataTableType(workspaceId)) {
      case RAWLS -> throw new NotImplementedException(
          "MultiCloudRecordSinkFactory does not support building a RawlsRecordSink from a CollectionId");
      case WDS -> new WdsRecordSink(recordService, recordDao, dataTypeInferer, collectionId);
    };
  }
}
