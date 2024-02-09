package org.databiosphere.workspacedataservice.recordstream;

import org.databiosphere.workspacedataservice.service.BatchWriteService.RecordSource;

/**
 * Marker interface that denotes a RecordSource which runs in two passes. The first pass will upsert
 * base attributes, and the second pass will upsert relation attributes. This allows for data
 * streams where earlier records contain relations to later records.
 */
public interface TwoPassRecordSource extends RecordSource {

  enum ImportMode {
    RELATIONS,
    BASE_ATTRIBUTES
  }
}
