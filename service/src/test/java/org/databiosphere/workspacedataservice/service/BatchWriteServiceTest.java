package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import bio.terra.pfb.PfbReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@TestPropertySource(properties = {"twds.write.batch.size=2"})
class BatchWriteServiceTest {

  @Autowired private BatchWriteService batchWriteService;
  @Autowired private InstanceDao instanceDao;
  @MockBean RecordDao recordDao;

  private static final UUID INSTANCE = UUID.fromString("aaaabbbb-cccc-dddd-1111-222233334444");
  private static final RecordType THING_TYPE = RecordType.valueOf("thing");

  @BeforeEach
  void setUp() {
    if (!instanceDao.instanceSchemaExists(INSTANCE)) {
      instanceDao.createSchema(INSTANCE);
    }
  }

  @AfterEach
  void tearDown() {
    instanceDao.dropSchema(INSTANCE);
  }

  @Test
  void testRejectsDuplicateKeys() {
    String streamContents =
        "[{\"operation\": \"upsert\", \"record\": {\"id\": \"1\", \"type\": \"thing\", \"attributes\": {\"key\": \"value1\", \"key\": \"value2\"}}}]";
    InputStream is = new ByteArrayInputStream(streamContents.getBytes());
    Optional<String> primaryKey = Optional.empty();

    Exception ex =
        assertThrows(
            BadStreamingWriteRequestException.class,
            () -> batchWriteService.batchWriteJsonStream(is, INSTANCE, THING_TYPE, primaryKey));

    String errorMessage = ex.getMessage();
    assertEquals("Duplicate field 'key'", errorMessage);
  }

  @Test
  void testParseMultipleTablesFromPfb() {
    /*
    Expected records:
           {
          "id": "HG01101_cram",
          "name": "submitted_aligned_reads",
          "object": {
            "file_format": "BAM",
            "error_type": "file_size",
            "file_name": "foo.bam",
            "file_size": 512,
            "file_state": "registered",
            "md5sum": "bdf121aadba028d57808101cb4455fa7",
            "object_id": "dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
            "created_datetime": null,
            "ga4gh_drs_uri": "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
            "participant_id": "bbb1234",
            "specimen_id": "spec1111",
            "experimental_strategy": null,
            "study_registration": "example.com/study_registration",
            "study_id": "aaa1234",
            "project_id": "DEV-test",
            "state": "uploading",
            "submitter_id": "HG01101_cram",
            "subject_id": "p1011554-9",
            "updated_datetime": null
          },
          "relations": []
        },
    {
        "id": "data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1",
        "name": "data_release",
        "object": {
          "created_datetime": "2022-06-01T00:00:00.000000Z",
          "updated_datetime": "2022-07-01T00:00:00.000000Z",
          "name": "0399fa30-30f5-4958-9726-9d7afe855f66",
          "major_version": 1,
          "minor_version": 1,
          "release_date": "2022-07-01",
          "released": false
          },
        "relations": []
      }
     */

    URL url = getClass().getResource("/two_tables.avro");
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      batchWriteService.batchWritePfbStream(dataStream, INSTANCE, Optional.of("id"));
      // TODO what and how exactly should i be verifying here?
      RecordAttributes expectedAttributes =
          new RecordAttributes(
              Map.of(
                  "created_datetime",
                  "2022-06-01T00:00:00.000000Z",
                  "updated_datetime",
                  "2022-07-01T00:00:00.000000Z",
                  "name",
                  "0399fa30-30f5-4958-9726-9d7afe855f66",
                  "major_version",
                  1,
                  "minor_version",
                  1,
                  "release_date",
                  "2022-07-01",
                  "released",
                  false));
      RecordType expectedRecordType = RecordType.valueOf("data_release");
      Record expectedRecord =
          new Record(
              "data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1",
              expectedRecordType,
              expectedAttributes);
      // TODO deal with data types, setting them to what actually happens for now
      Map<String, DataTypeMapping> expectedSchema =
          Map.of(
              "created_datetime",
              /*              DataTypeMapping.DATE_TIME,*/
              DataTypeMapping.STRING,
              "updated_datetime",
              /*              DataTypeMapping.DATE_TIME,*/
              DataTypeMapping.STRING,
              "name",
              DataTypeMapping.STRING,
              "major_version",
              /*              DataTypeMapping.NUMBER,*/
              DataTypeMapping.STRING,
              "minor_version",
              /*              DataTypeMapping.NUMBER,*/
              DataTypeMapping.STRING,
              "release_date",
              DataTypeMapping.DATE,
              "released",
              DataTypeMapping.BOOLEAN);
      verify(recordDao)
          .batchUpsert(INSTANCE, expectedRecordType, List.of(expectedRecord), expectedSchema, "id");

      RecordAttributes expectedAttributes2 =
          new RecordAttributes(
              Map.of(
                  "file_format",
                  "BAM",
                  "error_type",
                  "file_size",
                  "file_name",
                  "foo.bam",
                  "file_size",
                  512,
                  "file_state",
                  "registered",
                  "md5sum",
                  "bdf121aadba028d57808101cb4455fa7",
                  "object_id",
                  "dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
                  "ga4gh_drs_uri",
                  "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
                  "participant_id",
                  "bbb1234",
                  "specimen_id",
                  "spec1111"));
      // Split up since we can only do 10 values in a map at a time
      RecordAttributes bonusAttributes =
          new RecordAttributes(
              Map.of(
                  "study_registration", "example.com/study_registration",
                  "study_id", "aaa1234",
                  "project_id", "DEV-test",
                  "state", "uploading",
                  "submitter_id", "HG01101_cram",
                  "subject_id", "p1011554-9"));
      // Can't have null values in Map.of
      expectedAttributes2.putAttribute("created_datetime", null);
      expectedAttributes2.putAttribute("experimental_strategy", null);
      expectedAttributes2.putAttribute("updated_datetime", null);
      expectedAttributes2.putAll(bonusAttributes);
      RecordType expectedRecordType2 = RecordType.valueOf("submitted_aligned_reads");
      Record expectedRecord2 = new Record("HG01101_cram", expectedRecordType2, expectedAttributes2);
      // TODO deal with data types, setting them to what actually happens for now
      Map<String, DataTypeMapping> schemaValues1 =
          Map.of(
              "file_format", DataTypeMapping.STRING,
              "error_type", DataTypeMapping.STRING,
              "file_name", DataTypeMapping.STRING,
              /*              "file_size", DataTypeMapping.NUMBER,*/
              "file_size", DataTypeMapping.STRING,
              "file_state", DataTypeMapping.STRING,
              "md5sum", DataTypeMapping.STRING,
              "object_id", DataTypeMapping.STRING,
              "created_datetime", DataTypeMapping.NULL,
              "ga4gh_drs_uri", DataTypeMapping.FILE,
              "participant_id", DataTypeMapping.STRING);
      Map<String, DataTypeMapping> schemaValues2 =
          Map.of(
              "specimen_id",
              DataTypeMapping.STRING,
              "experimental_strategy",
              DataTypeMapping.NULL,
              "study_registration",
              DataTypeMapping.STRING,
              "study_id",
              DataTypeMapping.STRING,
              "project_id",
              DataTypeMapping.STRING,
              "state",
              DataTypeMapping.STRING,
              "submitter_id",
              DataTypeMapping.STRING,
              "subject_id",
              DataTypeMapping.STRING,
              "updated_datetime",
              DataTypeMapping.NULL);
      // Map.of is immutable
      Map<String, DataTypeMapping> expectedSchema2 = new HashMap<>();
      expectedSchema2.putAll(schemaValues1);
      expectedSchema2.putAll(schemaValues2);
      verify(recordDao)
          .batchUpsert(
              INSTANCE, expectedRecordType2, List.of(expectedRecord2), expectedSchema2, "id");
    } catch (IOException e) {
      fail(); // TODO failure message?
    }
  }

  // Tests tables across multiple batches
  @Test
  void testParsePfbMultipleBatches() {
    /*
    Expected records:
           {
          "id": "HG01101_cram",
          "name": "submitted_aligned_reads",
          "object": {
            "file_format": "BAM",
            "error_type": "file_size",
            "file_name": "foo.bam",
            "file_size": 512,
            "file_state": "registered",
            "md5sum": "bdf121aadba028d57808101cb4455fa7",
            "object_id": "dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
            "created_datetime": null,
            "ga4gh_drs_uri": "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
            "participant_id": "bbb1234",
            "specimen_id": "spec1111",
            "experimental_strategy": null,
            "study_registration": "example.com/study_registration",
            "study_id": "aaa1234",
            "project_id": "DEV-test",
            "state": "uploading",
            "submitter_id": "HG01101_cram",
            "subject_id": "p1011554-9",
            "updated_datetime": null
          },
          "relations": []
        },
    {
        "id": "data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1",
        "name": "data_release",
        "object": {
          "created_datetime": "2022-06-01T00:00:00.000000Z",
          "updated_datetime": "2022-07-01T00:00:00.000000Z",
          "name": "0399fa30-30f5-4958-9726-9d7afe855f66",
          "major_version": 1,
          "minor_version": 1,
          "release_date": "2022-07-01",
          "released": false
          },
        "relations": []
      },
      {
        "id": "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2",
        "name": "data_release",
        "object": {
          "created_datetime": "2022-06-02T00:00:00.000000Z",
          "updated_datetime": "2022-07-02T00:00:00.000000Z",
          "name": "9288ef29-29e4-3847-8615-8c6fed744e55",
          "major_version": 1,
          "minor_version": 2,
          "release_date": "2022-07-02",
          "released": false
        },
        "relations": []
      },
      {
        "id": "data_release.5733deca-0947-75b3-d854-f28c1819dcc7.3",
        "name": "data_release",
        "object": {
          "created_datetime": "2022-06-03T00:00:00.000000Z",
          "updated_datetime": "2022-07-02T00:00:00.000000Z",
          "name": "8177de18-18d3-2736-7504-7b5edc633d44",
          "major_version": 1,
          "minor_version": 2,
          "release_date": "2022-07-02",
          "released": false
        },
        "relations": []
      }
     */

    URL url = getClass().getResource("/four_tables.avro");
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      batchWriteService.batchWritePfbStream(dataStream, INSTANCE, Optional.of("id"));
      // TODO what and how exactly should i be verifying here?
      RecordType expectedRecordType = RecordType.valueOf("data_release");
      // Not checking the values since that's verified above, just that it gets called multiple
      // times
      // The first batch only has one data_release record
      verify(recordDao)
          .batchUpsert(
              eq(INSTANCE),
              eq(expectedRecordType),
              argThat(list -> list.size() == 1),
              any(),
              eq("id"));
      RecordType expectedRecordType2 = RecordType.valueOf("submitted_aligned_reads");
      verify(recordDao)
          .batchUpsert(
              eq(INSTANCE),
              eq(expectedRecordType2),
              argThat(list -> list.size() == 1),
              any(),
              eq("id"));
      // But we should call it again with a second batch of 2 data_release records
      verify(recordDao)
          .batchUpsert(
              eq(INSTANCE),
              eq(expectedRecordType),
              argThat(list -> list.size() == 2),
              any(),
              eq("id"));
    } catch (IOException e) {
      fail(); // TODO failure message?
    }
  }
}
