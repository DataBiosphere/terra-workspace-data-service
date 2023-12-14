package org.databiosphere.workspacedataservice.dataimport;

import bio.terra.datarepo.model.RelationshipModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/** Logic to convert a TDR Parquet's GenericRecord to WDS's Record */
// TODO AJ-1522 should ParquetRecordConverter be a bean, like PfbRecordConverter? Right now,
//     we construct separate ParquetRecordConverter instances for each table being imported.
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

  @Override
  String extractRecordId(GenericRecord genericRecord) {
    return genericRecord.get(idField).toString();
  }

  @Override
  RecordType extractRecordType(GenericRecord genericRecord) {
    return recordType;
  }

  @Override
  protected final RecordAttributes extractBaseAttributes(GenericRecord genericRecord) {
    // for base attributes, skip the id field and all relations
    List<String> relationNames =
        relationshipModels.stream().map(r -> r.getFrom().getColumn()).toList();
    Set<String> allIgnores = new HashSet<>();
    allIgnores.add(idField);
    allIgnores.addAll(relationNames);

    return super.baseAttributes(genericRecord, allIgnores);
  }

  @Override
  protected final RecordAttributes extractRelations(GenericRecord genericRecord) {
    // find relation columns for this type
    if (relationshipModels.isEmpty()) {
      return RecordAttributes.empty();
    }

    RecordAttributes attributes = RecordAttributes.empty();

    // loop through relation columns
    relationshipModels.forEach(
        relationshipModel -> {
          String attrName = relationshipModel.getFrom().getColumn();
          // get value from Avro
          Object value = genericRecord.get(attrName);
          if (value != null) {
            String targetType = relationshipModel.getTo().getTable();
            // is it an array?
            if (value instanceof Collection<?> relArray) {
              List<String> rels =
                  relArray.stream()
                      .map(
                          relValue ->
                              RelationUtils.createRelationString(
                                  RecordType.valueOf(targetType), relValue.toString()))
                      .toList();
              attributes.putAttribute(attrName, rels);
            } else {
              attributes.putAttribute(
                  attrName,
                  RelationUtils.createRelationString(
                      RecordType.valueOf(targetType), value.toString()));
            }
          }
        });

    return attributes;
  }
}
