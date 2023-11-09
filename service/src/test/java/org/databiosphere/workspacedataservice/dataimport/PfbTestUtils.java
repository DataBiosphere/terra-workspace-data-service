package org.databiosphere.workspacedataservice.dataimport;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class PfbTestUtils {

  // define the avro schema for the "object" field, expecting no additional attributes.
  public static Schema OBJECT_SCHEMA =
      Schema.createRecord("objectSchema", "doc", "namespace", false, List.of());

  // define the avro schema for the top-level fields expected in the PFB: id, name, object
  public static Schema RECORD_SCHEMA =
      Schema.createRecord(
          "recordSchema",
          "doc",
          "namespace",
          false,
          List.of(
              new Schema.Field("id", Schema.create(Schema.Type.STRING)),
              new Schema.Field("name", Schema.create(Schema.Type.STRING)),
              new Schema.Field("object", OBJECT_SCHEMA)));

  /**
   * Create a GenericRecord with the given id and name and an empty set of object attributes
   *
   * @param id "id" column in the PFB; translates to a WDS record id.
   * @param name "name" column in the PFB; translates to a WDS record type.
   * @return the GenericRecord
   */
  public static GenericRecord makeRecord(String id, String name) {
    return makeRecord(id, name, new GenericData.Record(OBJECT_SCHEMA));
  }

  /**
   * Create a GenericRecord with the given id, name, and object attributes. Callers of this method
   * will have to define their own avro Schema object for whatever objectAttributes they want to
   * pass in.
   *
   * @param id "id" column in the PFB; translates to a WDS record id.
   * @param name "name" column in the PFB; translates to a WDS record type.
   * @param objectAttributes "object" column in the PFB; translates to WDS record attributes.
   * @return the GenericRecord
   */
  public static GenericRecord makeRecord(
      String id, String name, GenericData.Record objectAttributes) {
    GenericRecord rec = new GenericData.Record(RECORD_SCHEMA);
    rec.put("id", id);
    rec.put("name", name);
    rec.put("object", objectAttributes);
    return rec;
  }
}
