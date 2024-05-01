package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.springframework.stereotype.Component;

/** RecordSinkFactory implementation for the control plane */
@ControlPlane
@Component
public class RawlsRecordSinkFactory implements RecordSinkFactory {
  private final ObjectMapper mapper;

  private final GcsStorage storage;

  private final PubSub pubSub;

  public RawlsRecordSinkFactory(ObjectMapper mapper, GcsStorage storage, PubSub pubSub) {
    this.mapper = mapper;
    this.storage = storage;
    this.pubSub = pubSub;
  }

  public RecordSink buildRecordSink(ImportDetails importDetails) {
    return rawlsRecordSink(importDetails);
  }

  @Override
  public RecordSink buildRecordSink(CollectionId collectionId) {
    throw new NotImplementedException(
        "RawlsRecordSinkFactory does not support building a RecordSink from a CollectionId");
  }

  private RecordSink rawlsRecordSink(ImportDetails importDetails) {
    return RawlsRecordSink.create(mapper, storage, pubSub, importDetails);
  }
}
