package org.databiosphere.workspacedataservice.dataimport;

import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert a TDR Parquet's GenericRecord to WDS's Record */
public class ParquetRecordConverter extends AvroRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetRecordConverter.class);

  private final RecordType recordType;
  private final String idField;

  public ParquetRecordConverter(RecordType recordType, String idField) {
    super();
    this.recordType = recordType;
    this.idField = idField;
  }

  @Override
  Record createRecordShell(GenericRecord genRec) {
    return new Record(genRec.get(idField).toString(), recordType, RecordAttributes.empty());
  }

  @Override
  protected final Record addAttributes(GenericRecord objectAttributes, Record converted) {
    return super.addAttributes(objectAttributes, converted, Set.of(idField));
  }

  @Override
  protected final Record addRelations(GenericRecord genRec, Record converted) {
    // TODO AJ-1013 implement relations for TDR import
    return converted;
  }
}
