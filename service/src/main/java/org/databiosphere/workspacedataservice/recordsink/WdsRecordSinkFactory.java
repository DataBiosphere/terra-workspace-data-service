package org.databiosphere.workspacedataservice.recordsink;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.springframework.stereotype.Component;

/** RecordSinkFactory implementation for the data plane */
@DataPlane
@Component
public class WdsRecordSinkFactory implements RecordSinkFactory {
  private final RecordService recordService;
  private final RecordDao recordDao;
  private final DataTypeInferer dataTypeInferer;

  public WdsRecordSinkFactory(
      RecordService recordService, RecordDao recordDao, DataTypeInferer dataTypeInferer) {
    this.recordService = recordService;
    this.recordDao = recordDao;
    this.dataTypeInferer = dataTypeInferer;
  }

  // TODO(AJ-1589): make prefix assignment dynamic. However, of note: the prefix is currently
  //   ignored for RecordSinkMode.WDS.  In this case, it might be worth adding support for omitting
  //   the prefix as part of supporting the prefix assignment.
  public RecordSink buildRecordSink(ImportDetails importDetails) {
    return wdsRecordSink(importDetails.workspaceId());
  }

  private WdsRecordSink wdsRecordSink(UUID collectionId) {
    return new WdsRecordSink(recordService, recordDao, dataTypeInferer, collectionId);
  }
}
