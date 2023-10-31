package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class PfbStreamWriteHandlerTest {

  @Test
  void pfbTablesAreParsedCorrectly() {
    URL url = getClass().getResource("/two_tables.avro");
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
      // TODO what to do about these being whatever object type
      assertEquals("aaa1234", firstRecord.getAttributeValue("study_id").toString());
      assertEquals("512", firstRecord.getAttributeValue("file_size"));

      Record secondRecord = result.get(1);
      assertNotNull(secondRecord);
      assertEquals("data_release.3511bcae-8725-53f1-b632-d06a9697baa5.1", secondRecord.getId());
      assertEquals(RecordType.valueOf("data_release"), secondRecord.getRecordType());
      assertEquals(7, secondRecord.attributeSet().size());
      assertEquals(
          "2022-07-01T00:00:00.000000Z",
          secondRecord.getAttributeValue("updated_datetime").toString());
      assertEquals("1", secondRecord.getAttributeValue("minor_version"));
    } catch (IOException e) {
      fail();
    }
  }
}
