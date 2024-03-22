package org.databiosphere.workspacedataservice.recordsink;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
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

  // TODO(AJ-1589): make prefix assignment dynamic. However, of note: the prefix is currently
  //   ignored for RecordSinkMode.WDS.  In this case, it might be worth adding support for omitting
  //   the prefix as part of supporting the prefix assignment.
  public RecordSink buildRecordSink(ImportDetails importDetails) {
    return rawlsRecordSink(importDetails);
  }

  private RecordSink rawlsRecordSink(ImportDetails importDetails) {
    requireNonNull(
        importDetails.jobId(), "RawlsRecordSink requires ImportDetails.jobId to be non-null");
    requireNonNull(
        importDetails.userEmailSupplier(),
        "RawlsRecordSink requires ImportDetails.userEmailSupplier to be non-null");
    return RawlsRecordSink.create(mapper, storage, pubSub, importDetails);
  }
}
