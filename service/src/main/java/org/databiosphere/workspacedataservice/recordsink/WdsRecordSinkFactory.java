package org.databiosphere.workspacedataservice.recordsink;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
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

  public RecordSink buildRecordSink(ImportDetails importDetails) {
    return buildRecordSink(importDetails.collectionId());
  }

  @Override
  public RecordSink buildRecordSink(CollectionId collectionId) {
    return new WdsRecordSink(recordService, recordDao, dataTypeInferer, collectionId);
  }
}
