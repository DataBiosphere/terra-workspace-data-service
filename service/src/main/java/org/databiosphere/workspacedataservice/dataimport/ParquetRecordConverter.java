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
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

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

  @Override
  Record createRecordShell(GenericRecord genRec) {
    return new Record(genRec.get(idField).toString(), recordType, RecordAttributes.empty());
  }

  @Override
  protected final Record addAttributes(GenericRecord objectAttributes, Record converted) {
    // for base attributes, skip the id field and all relations
    List<String> relationNames =
        relationshipModels.stream().map(r -> r.getFrom().getColumn()).toList();
    Set<String> allIgnores = new HashSet<>();
    allIgnores.add(idField);
    allIgnores.addAll(relationNames);

    return super.addAttributes(objectAttributes, converted, allIgnores);
  }

  @Override
  protected final Record addRelations(GenericRecord genRec, Record converted) {
    // find relation columns for this type
    if (relationshipModels.isEmpty()) {
      return converted;
    }

    RecordAttributes attributes = RecordAttributes.empty();

    // loop through relation columns
    relationshipModels.forEach(
        relationshipModel -> {
          String attrName = relationshipModel.getFrom().getColumn();
          // get value from Avro
          Object value = genRec.get(attrName);
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

    converted.setAttributes(attributes);

    return converted;
  }
}
