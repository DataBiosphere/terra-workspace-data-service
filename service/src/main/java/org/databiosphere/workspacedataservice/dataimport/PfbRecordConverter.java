package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter extends AvroRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PfbRecordConverter.class);

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";
  public static final String RELATIONS_FIELD = "relations";
  public static final String RELATIONS_ID = "dst_id";
  public static final String RELATIONS_NAME = "dst_name";

  public PfbRecordConverter(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  Record createRecordShell(GenericRecord genRec) {
    return new Record(
        genRec.get(ID_FIELD).toString(),
        RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
        RecordAttributes.empty());
  }

  @Override
  protected final Record addAttributes(GenericRecord genRec, Record converted) {
    // extract the OBJECT_FIELD sub-record, then find all its attributes
    if (genRec.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
      return super.addAttributes(objectAttributes, converted, Set.of());
    }
    return converted;
  }

  @Override
  protected final Record addRelations(GenericRecord genRec, Record converted) {
    // get the relations array from the record
    if (genRec.get(RELATIONS_FIELD) instanceof Collection<?> relationArray
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
              relationType,
              RelationUtils.createRelationString(RecordType.valueOf(relationType), relationId));
        }
      }
      converted.setAttributes(attributes);
    }
    return converted;
  }
}
