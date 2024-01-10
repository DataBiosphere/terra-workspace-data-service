package org.databiosphere.workspacedataservice.recordstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.mockPfbStream;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.pfb.PfbReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode;
import org.databiosphere.workspacedataservice.service.JsonConfig;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {JsonConfig.class})
class PfbStreamWriteHandlerTest {
  @Autowired private ObjectMapper objectMapper;

  // does PfbStreamWriteHandler properly know how to page through a DataFileStream<GenericRecord>?
  @Test
  void testBatching() {
    // create a mock PFB stream with 10 rows in it and a PfbStreamWriteHandler for that stream
    PfbStreamWriteHandler pfbStreamWriteHandler =
        buildHandler(
            mockPfbStream(10, "someType"), TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES);

    StreamingWriteHandler.WriteStreamInfo batch; // used in assertions below

    // ask for the first 2/10 rows, should get those two back
    batch = pfbStreamWriteHandler.readRecords(2);
    assertEquals(2, batch.getRecords().size());
    assertEquals(List.of("0", "1"), batch.getRecords().stream().map(Record::getId).toList());

    // ask for the next 6/10, should skip the first two that have already been consumed and
    // return the next 6
    batch = pfbStreamWriteHandler.readRecords(6);
    assertEquals(6, batch.getRecords().size());
    assertEquals(
        List.of("2", "3", "4", "5", "6", "7"),
        batch.getRecords().stream().map(Record::getId).toList());

    // ask for 12 more. But since there are only 2 remaining in the source, we'll just get 2 back
    batch = pfbStreamWriteHandler.readRecords(12);
    assertEquals(2, batch.getRecords().size());
    assertEquals(List.of("8", "9"), batch.getRecords().stream().map(Record::getId).toList());
  }

  // does PfbStreamWriteHandler handle inputs of various sizes correctly?
  @ParameterizedTest(name = "dont error if input has {0} row(s)")
  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101})
  void inputStreamOfCount(Integer numRows) {
    PfbStreamWriteHandler pfbStreamWriteHandler =
        buildHandler(
            mockPfbStream(numRows, "someType"),
            TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES);

    int batchSize = 50;

    assertDoesNotThrow(
        () -> {
          for (StreamingWriteHandler.WriteStreamInfo info =
                  pfbStreamWriteHandler.readRecords(batchSize);
              !info.getRecords().isEmpty();
              info = pfbStreamWriteHandler.readRecords(batchSize)) {
            assertThat(info.getRecords()).hasSizeLessThanOrEqualTo(batchSize);
          }
        });
  }

  @Test
  // Given a real PFB file, does PfbStreamWriteHandler faithfully return the records inside that
  // PFB?
  void pfbTablesAreParsedCorrectly() {
    try (DataFileStream<GenericRecord> dataFileStream = streamRecordsFromFile("/two_tables.avro")) {
      PfbStreamWriteHandler pswh = buildHandler(dataFileStream, ImportMode.BASE_ATTRIBUTES);
      StreamingWriteHandler.WriteStreamInfo streamInfo = pswh.readRecords(2);
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
      List<Record> result = streamInfo.getRecords();
      assertEquals(2, result.size());
      Record firstRecord = result.get(0);
      assertNotNull(firstRecord);
      assertEquals("HG01101_cram", firstRecord.getId());
      assertEquals(RecordType.valueOf("submitted_aligned_reads"), firstRecord.getRecordType());
      assertEquals(19, firstRecord.attributeSet().size());

      // smoke-test a few values
      assertEquals(BigDecimal.valueOf(512), firstRecord.getAttributeValue("file_size"));
      assertEquals("registered", firstRecord.getAttributeValue("file_state"));
      assertEquals(
          "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
          firstRecord.getAttributeValue("ga4gh_drs_uri"));

      Record secondRecord = result.get(1);
      assertNotNull(secondRecord);
      assertEquals("data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1", secondRecord.getId());
      assertEquals(RecordType.valueOf("data_release"), secondRecord.getRecordType());
      assertEquals(7, secondRecord.attributeSet().size());
    } catch (IOException e) {
      fail(e);
    }
  }

  @Test
  void relationsAreParsedCorrectly() {
    try (DataFileStream<GenericRecord> dataFileStream = streamRecordsFromFile("/test.avro")) {

      StreamingWriteHandler.WriteStreamInfo streamInfo =
          buildHandler(dataFileStream, ImportMode.RELATIONS).readRecords(5);

      List<Record> result = streamInfo.getRecords();
      assertEquals(5, result.size());
      Record firstRecord = result.get(0);
      // The first record does not have any relations
      assertNotNull(firstRecord);
      assertEquals("activities.34f8be82-2973-52c8-ad95-ba79416c51ab.3", firstRecord.getId());
      assertEquals(0, firstRecord.attributeSet().size());

      // validate that relations were identified
      Record secondRecord = result.get(4);
      assertNotNull(secondRecord);
      assertEquals("files.3511bcae-8725-53f1-b632-d06a9697baa5.1", secondRecord.getId());
      // this record has 4 relations
      assertEquals(4, secondRecord.attributeSet().size());
      assertEquals(
          RelationUtils.createRelationString(
              RecordType.valueOf("activities"),
              "activities.34f8be82-2973-52c8-ad95-ba79416c51ab.3"),
          secondRecord.getAttributeValue("activities"));
      assertEquals(
          RelationUtils.createRelationString(
              RecordType.valueOf("biosamples"),
              "biosamples.30a60040-fdca-5473-b2d5-cd3839e983c7.1"),
          secondRecord.getAttributeValue("biosamples"));
      assertEquals(
          RelationUtils.createRelationString(
              RecordType.valueOf("datasets"), "datasets.cd90e9d7-4b3c-5705-bcbf-d477af2c4f7d.1"),
          secondRecord.getAttributeValue("datasets"));
      assertEquals(
          RelationUtils.createRelationString(
              RecordType.valueOf("donors"), "donors.cce44986-0d04-54ea-8343-83748cd7225a.1"),
          secondRecord.getAttributeValue("donors"));
    } catch (IOException e) {
      fail(e);
    }
  }

  private DataFileStream<GenericRecord> streamRecordsFromFile(String fileName) throws IOException {
    URL url = getClass().getResource(fileName);
    assertNotNull(url);
    return PfbReader.getGenericRecordsStream(url.toString());
  }

  private PfbStreamWriteHandler buildHandler(
      DataFileStream<GenericRecord> dataFileStream, ImportMode importMode) {
    return new PfbStreamWriteHandler(dataFileStream, importMode, objectMapper);
  }
}
