package org.databiosphere.workspacedataservice.dataimport.tdr;

import bio.terra.datarepo.model.RelationshipModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.AvroRecordConverter;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;

/** Logic to convert a TDR Parquet's GenericRecord to WDS's Record */
public class ParquetRecordConverter extends AvroRecordConverter {
  private final RecordType recordType;
  private final String idField;
  private final List<RelationshipModel> relationshipModels;

  public ParquetRecordConverter(TdrManifestImportTable table, ObjectMapper objectMapper) {
    super(objectMapper);
    this.recordType = table.recordType();
    this.idField = table.primaryKey();
    this.relationshipModels = table.relations();
  }

  private Record createEmptyRecord(GenericRecord genericRecord) {
    return new Record(genericRecord.get(idField).toString(), recordType);
  }

  @Override
  protected final Record convertBaseAttributes(GenericRecord genericRecord) {
    Record record = createEmptyRecord(genericRecord);
    // for base attributes, skip the id field and all relations
    List<String> relationNames =
        relationshipModels.stream().map(r -> r.getFrom().getColumn()).toList();
    Set<String> allIgnores = new HashSet<>(relationNames);

    record.setAttributes(extractBaseAttributes(genericRecord, allIgnores));

    return record;
  }

  @Override
  protected final Record convertRelations(GenericRecord genericRecord) {
    Record record = createEmptyRecord(genericRecord);
    // find relation columns for this type
    if (relationshipModels.isEmpty()) {
      return record;
    }

    RecordAttributes attributes = RecordAttributes.empty();

    // filter the relationship models to those known by this schema. This should only reduce the
    // list in strange conditions in which the TDR schema is out of sync with the Parquet schema.
    List<RelationshipModel> knownModels =
        relationshipModels.stream()
            .filter(
                model ->
                    genericRecord.getSchema().hasFields()
                        && genericRecord.hasField(model.getFrom().getColumn()))
            .toList();

    // loop through relation columns
    knownModels.forEach(
        relationshipModel -> {
          String attrName = relationshipModel.getFrom().getColumn();
          // get field definition from Avro. The call to getField() here is non-null because
          // we filter relationship models above.
          Schema.Field field = genericRecord.getSchema().getField(attrName);
          // get value from Avro
          Object value = destructureElementList(genericRecord.get(attrName), field);
          if (value != null) {
            String targetType = relationshipModel.getTo().getTable();
            // is it an array?
            if (value instanceof Collection<?> relArray) {
              List<RelationAttribute> rels =
                  relArray.stream()
                      .map(
                          relValue ->
                              new RelationAttribute(
                                  RecordType.valueOf(targetType), relValue.toString()))
                      .toList();
              attributes.putAttribute(attrName, rels);
            } else {
              attributes.putAttribute(
                  attrName,
                  new RelationAttribute(RecordType.valueOf(targetType), value.toString()));
            }
          }
        });

    record.setAttributes(attributes);

    return record;
  }
}
