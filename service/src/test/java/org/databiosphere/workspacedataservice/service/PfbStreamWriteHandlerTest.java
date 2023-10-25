package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

public class PfbStreamWriteHandlerTest {

  @Test
  void pfbTablesAreParsedCorrectly() throws IOException {
    URL url = getClass().getResource("/minimal_data.avro");
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      // translate the Avro DataFileStream into a Java stream
      Stream<GenericRecord> recordStream =
          StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(dataStream.iterator(), Spliterator.ORDERED),
              false);

      PfbStreamWriteHandler pswh = new PfbStreamWriteHandler(recordStream);
      StreamingWriteHandler.WriteStreamInfo streamInfo = pswh.readRecords(1);
      /*
      Expected first record:
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
      }
       */
      List<Record> result = streamInfo.getRecords();
      Record firstRecord = result.get(0);
      assertNotNull(firstRecord);
      assertEquals("HG01101_cram", firstRecord.getId());
      assertEquals(RecordType.valueOf("submitted_aligned_reads"), firstRecord.getRecordType());
      assertEquals(19, firstRecord.attributeSet().size());
      // just a spot check for now
      // TODO what to do about these being whatever object type
      assertEquals("aaa1234", firstRecord.getAttributeValue("study_id").toString());
      assertEquals(512L, firstRecord.getAttributeValue("file_size"));
    } catch (IOException e) {
      fail();
    }
  }
}
