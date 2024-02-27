package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.function.Consumer;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.recordsink.RawlsRecordSink.RawlsJsonConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** RecordSinkFactory implementation for the control plane */
@ControlPlane
@Component
public class RawlsRecordSinkFactory implements RecordSinkFactory {

  private Consumer<String> jsonConsumer;

  private final ObjectMapper mapper;

  public RawlsRecordSinkFactory(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  // jsonConsumer currently only used by tests, so it is optional. If/when this is used consistently
  // at runtime, it should move to the constructor and no longer be optional
  @Autowired(required = false)
  public void setJsonConsumer(@RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.jsonConsumer = jsonConsumer;
  }

  // TODO(AJ-1589): make prefix assignment dynamic. However, of note: the prefix is currently
  //   ignored for RecordSinkMode.WDS.  In this case, it might be worth adding support for omitting
  //   the prefix as part of supporting the prefix assignment.
  public RecordSink buildRecordSink(UUID collectionId, String prefix) {
    return rawlsRecordSink(prefix);
  }

  private RecordSink rawlsRecordSink(String prefix) {
    return new RawlsRecordSink(prefix, mapper, jsonConsumer);
  }
}
