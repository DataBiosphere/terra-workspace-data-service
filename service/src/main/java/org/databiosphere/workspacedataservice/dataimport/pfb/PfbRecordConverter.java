package org.databiosphere.workspacedataservice.dataimport.pfb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.AvroRecordConverter;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter extends AvroRecordConverter {

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";
  public static final String RELATIONS_FIELD = "relations";
  public static final String RELATIONS_ID = "dst_id";
  public static final String RELATIONS_NAME = "dst_name";

  public PfbRecordConverter(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  private Record createEmptyRecord(GenericRecord genericRecord) {
    return new Record(
        genericRecord.get(ID_FIELD).toString(),
        RecordType.valueOf(genericRecord.get(TYPE_FIELD).toString()));
  }

  @Override
  protected final Record convertBaseAttributes(GenericRecord genericRecord) {
    Record record = createEmptyRecord(genericRecord);
    // extract the OBJECT_FIELD sub-record, then find all its attributes
    if (genericRecord.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
      RecordAttributes attributes = extractBaseAttributes(objectAttributes, Set.of());
      record.setAttributes(attributes);
    }
    return record;
  }

  @Override
  protected final Record convertRelations(GenericRecord genericRecord) {
    Record record = createEmptyRecord(genericRecord);
    // get the relations array from the record
    if (genericRecord.get(RELATIONS_FIELD) instanceof Collection<?> relationArray
        && !relationArray.isEmpty()) {
      RecordAttributes attributes = RecordAttributes.empty();
      for (Object relationObject : relationArray) {
        // Here we assume that the relations object is a GenericRecord with keys "dst_name" and
        // "dst_id"
        if (relationObject instanceof GenericRecord relation) {
          String relationType = relation.get(RELATIONS_NAME).toString();
          String relationId = relation.get(RELATIONS_ID).toString();
          // Give the relation column the name of the record type it's linked to
          attributes.putAttribute(
              relationType, new RelationAttribute(RecordType.valueOf(relationType), relationId));
        }
      }
      record.setAttributes(attributes);
    }
    return record;
  }
}
