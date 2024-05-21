package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.RELATIONS_ID;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.RELATIONS_NAME;
import static org.databiosphere.workspacedataservice.service.ImportService.ARG_IMPORT_JOB_INPUT;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.mockito.Mockito;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.springframework.core.io.Resource;

public class PfbTestUtils {

  // define the avro schema for the "object" field, expecting no additional attributes.
  public static Schema OBJECT_SCHEMA =
      Schema.createRecord("objectSchema", "doc", "namespace", false, List.of());
  // define the avro schema for a "relation" and the array that holds it
  public static Schema RELATION_SCHEMA =
      Schema.createRecord(
          "relationSchema",
          "doc",
          "namespace",
          false,
          List.of(
              new Schema.Field(RELATIONS_ID, Schema.create(Schema.Type.STRING)),
              new Schema.Field(RELATIONS_NAME, Schema.create(Schema.Type.STRING))));
  public static Schema RELATION_ARRAY_SCHEMA = SchemaBuilder.array().items(RELATION_SCHEMA);

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
              new Schema.Field("object", OBJECT_SCHEMA),
              new Schema.Field("relations", RELATION_ARRAY_SCHEMA)));

  public static final String BEARER_TOKEN = "expectedToken";

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
   * Create a GenericRecord with the given id, name, and object attributes. Relations will be an
   * empty array. Callers of this method will have to define their own avro Schema object for
   * whatever objectAttributes they want to pass in.
   *
   * @param id "id" column in the PFB; translates to a WDS record id.
   * @param name "name" column in the PFB; translates to a WDS record type.
   * @param objectAttributes "object" column in the PFB; translates to WDS record attributes.
   * @return the GenericRecord
   */
  public static GenericRecord makeRecord(
      String id, String name, GenericData.Record objectAttributes) {
    return makeRecord(
        id, name, objectAttributes, new GenericData.Array(RELATION_ARRAY_SCHEMA, List.of()));
  }

  /**
   * Create a GenericRecord with the given id, name, object attributes, and relations. Callers of
   * this method will have to define their own avro Schema object for whatever objectAttributes they
   * want to pass in.
   *
   * @param id "id" column in the PFB; translates to a WDS record id.
   * @param name "name" column in the PFB; translates to a WDS record type.
   * @param objectAttributes "object" column in the PFB; translates to WDS record attributes.
   * @param relations "relation" array indicating relations between records
   * @return the GenericRecord
   */
  public static GenericRecord makeRecord(
      String id, String name, GenericData.Record objectAttributes, GenericData.Array relations) {
    return new GenericRecordBuilder(RECORD_SCHEMA)
        .set("id", id)
        .set("name", name)
        .set("object", objectAttributes)
        .set("relations", relations)
        .build();
  }

  public static DataFileStream<GenericRecord> mockPfbStream(int numRows, String name) {
    Map<String, Integer> counts = Map.of(name, numRows);
    return mockPfbStream(counts);
  }

  /**
   * Helper method: create a Mockito mock of a DataFileStream<GenericRecord>, containing `numRows`
   * PFB records. Each record has an id, name, and object attributes. The id is equal to its index
   * in numRows: if you request a mock with three rows, it will have records with ids 0, 1, 2.
   *
   * <p>If you care about the order in which the record types appear in the stream, make sure to use
   * an ordered Map such as a TreeMap
   *
   * @param counts map of type->numRows describing the records to return
   */
  public static DataFileStream<GenericRecord> mockPfbStream(Map<String, Integer> counts) {
    // for each key in `counts`, create a list of ${numRows} GenericRecords, whose id is their index
    List<GenericRecord> records = new ArrayList<>();
    counts.forEach(
        (name, numRows) -> {
          records.addAll(
              IntStream.range(0, numRows)
                  .mapToObj(i -> PfbTestUtils.makeRecord(Integer.valueOf(i).toString(), name))
                  .toList());
        });

    // create the Mockito mock for DataFileStream with implementations of hasNext() and next()
    // that pull from the head of the ${records} list
    @SuppressWarnings("unchecked")
    DataFileStream<GenericRecord> dataFileStream = Mockito.mock(DataFileStream.class);

    when(dataFileStream.hasNext()).thenAnswer(invocation -> !records.isEmpty());
    when(dataFileStream.next())
        .thenAnswer(
            invocation -> {
              GenericRecord rec = records.get(0);
              records.remove(0);
              return rec;
            });

    return dataFileStream;
  }

  public static JobExecutionContext stubJobContext(UUID jobId, Resource resource, UUID collectionId)
      throws IOException {
    return stubJobContext(jobId, resource.getURI(), collectionId);
  }

  public static JobExecutionContext stubJobContext(UUID jobId, URI resourceUri, UUID collectionId) {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);

    ImportJobInput importJobInput =
        new ImportJobInput(
            URI.create("https://data.terra.bio/manifest.json"),
            TypeEnum.PFB,
            new PfbImportOptions());

    var schedulable =
        ImportService.createSchedulable(
            ImportRequestServerModel.TypeEnum.PFB,
            jobId,
            new ImmutableMap.Builder<String, Serializable>()
                .put(ARG_TOKEN, BEARER_TOKEN)
                .put(ARG_URL, resourceUri.toString())
                .put(ARG_COLLECTION, collectionId.toString())
                .put(ARG_IMPORT_JOB_INPUT, importJobInput)
                .build());

    JobDetail jobDetail = schedulable.getJobDetail();
    when(mockContext.getMergedJobDataMap()).thenReturn(jobDetail.getJobDataMap());
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    return mockContext;
  }
}
