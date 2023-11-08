package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PfbStreamWriteHandlerTest {

  private DataFileStream<GenericRecord> mockPfbStream(int numRows) {
    // define the avro schema for the "object" field
    Schema objectSchema =
        Schema.createRecord(
            "object",
            "doc",
            "namespace",
            false,
            List.of(new Schema.Field("whatever", Schema.create(Schema.Type.STRING))));

    // define the avro schema for the top-level fields: id, name, object
    Schema schema =
        Schema.createRecord(
            "name",
            "doc",
            "namespace",
            false,
            List.of(
                new Schema.Field("id", Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("object", objectSchema)));

    // create a list of ${numRows} GenericRecords, whose id is their index
    List<GenericRecord> records =
        new java.util.ArrayList<>(
            IntStream.range(0, numRows)
                .mapToObj(
                    i -> {
                      GenericRecord rec = new GenericData.Record(schema);
                      rec.put("name", "bar");
                      rec.put("id", i);
                      rec.put("object", new GenericData.Record(objectSchema));
                      return rec;
                    })
                .toList());

    // create a Mockito mock for DataFileStream with implementations of hasNext() and next()
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

  // does PfbStreamWriteHandler properly know how to page through a DataFileStream<GenericRecord>?
  @Test
  void testBatching() throws IOException {

    // create a mock PFB stream with 10 rows in it and a PfbStreamWriteHandler for that stream
    DataFileStream<GenericRecord> dataFileStream = mockPfbStream(10);
    PfbStreamWriteHandler pfbStreamWriteHandler = new PfbStreamWriteHandler(dataFileStream);

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

  @Test
  // TODO AJ-1227: assess
  void pfbTablesAreParsedCorrectly() {
    URL url = getClass().getResource("/two_tables.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {

      PfbStreamWriteHandler pswh = new PfbStreamWriteHandler(dataStream);
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
      // just a spot check for now
      // TODO AJ-1227: what to do about these being whatever object type
      assertEquals("aaa1234", firstRecord.getAttributeValue("study_id").toString());
      assertEquals(512L, firstRecord.getAttributeValue("file_size"));

      Record secondRecord = result.get(1);
      assertNotNull(secondRecord);
      assertEquals("data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1", secondRecord.getId());
      assertEquals(RecordType.valueOf("data_release"), secondRecord.getRecordType());
      assertEquals(7, secondRecord.attributeSet().size());
      assertEquals(
          "2022-07-01T00:00:00.000000Z",
          secondRecord.getAttributeValue("updated_datetime").toString());
      assertEquals(1L, secondRecord.getAttributeValue("minor_version"));
    } catch (IOException e) {
      fail();
    }
  }
}
