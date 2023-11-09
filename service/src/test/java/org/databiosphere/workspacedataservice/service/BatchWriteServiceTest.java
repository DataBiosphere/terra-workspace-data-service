package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter.ID_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import bio.terra.pfb.PfbReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.PfbTestUtils;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@TestPropertySource(properties = {"twds.write.batch.size=2"})
class BatchWriteServiceTest {

  @Autowired private BatchWriteService batchWriteService;
  @Autowired private InstanceDao instanceDao;
  @MockBean RecordDao recordDao;
  @SpyBean DataTypeInferer inferer;
  @SpyBean RecordService recordService;

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

  // when batchWriteTsvStream is called with a single specified RecordType, we should infer the
  // table schema only once.
  @Test
  void schemaInferredOnceForOneRecordType() throws IOException {
    // generate a TSV input with a header and 5 rows
    StringBuilder tsvContent = new StringBuilder("id\tcol1\n");
    for (int i = 0; i < 5; i++) {
      tsvContent.append(i).append("\t").append(i).append("\n");
    }
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContent.toString().getBytes());

    // other params
    RecordType recordType = RecordType.valueOf("myType");
    Optional<String> primaryKey = Optional.of("id");

    // call the BatchWriteService. Since this test specifies a batch size of 2, the 5 TSV rows will
    // execute in 3 batches.
    // Note that this call to batchWriteTsvStream specifies a non-null RecordType.
    batchWriteService.batchWriteTsvStream(file.getInputStream(), INSTANCE, recordType, primaryKey);

    // we should write three batches
    verify(recordService, times(3))
        .batchUpsertWithErrorCapture(
            eq(INSTANCE), eq(recordType), any(), any(), eq(primaryKey.get()));

    // but we should only have inferred the schema once
    verify(inferer, times(1)).inferTypes(ArgumentMatchers.<List<Record>>any());
  }

  // when batchWritePfbStream is called and the input contains multiple
  // record types, we should infer each type's table schema only once.
  @Test
  void schemaInferredOnceForEachRecordType() throws IOException {

    Optional<String> primaryKey = Optional.of(ID_FIELD);

    // create a stream with 5 "things", 10 "items", and 15 "widgets"
    Map<String, Integer> counts = Map.of("thing", 5, "item", 10, "widget", 15);
    try (DataFileStream<GenericRecord> pfbStream = PfbTestUtils.mockPfbStream(counts)) {

      // call the BatchWriteService. Since this test specifies a batch size of 2, this
      // should call:
      // * batchUpsertWithErrorCapture 3 times for "thing"
      // * batchUpsertWithErrorCapture 5 times for "item"
      // * batchUpsertWithErrorCapture 8 times for "widget"
      // * inferTypes once for each of "thing", "item", and "widget"
      batchWriteService.batchWritePfbStream(pfbStream, INSTANCE, Optional.of(ID_FIELD));

      // verify calls to batchUpsertWithErrorCapture
      verify(recordService, times(3))
          .batchUpsertWithErrorCapture(
              eq(INSTANCE), eq(RecordType.valueOf("thing")), any(), any(), eq(primaryKey.get()));
      verify(recordService, times(6))
          .batchUpsertWithErrorCapture(
              eq(INSTANCE), eq(RecordType.valueOf("item")), any(), any(), eq(primaryKey.get()));
      verify(recordService, times(8))
          .batchUpsertWithErrorCapture(
              eq(INSTANCE), eq(RecordType.valueOf("widget")), any(), any(), eq(primaryKey.get()));

      // but we should only have inferred schemas three times - once for each record Type
      @SuppressWarnings("unchecked")
      ArgumentCaptor<List<Record>> argumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(inferer, times(3)).inferTypes(argumentCaptor.capture());

      List<List<Record>> allArguments = argumentCaptor.getAllValues();
      // collapse the record types from each call to inferTypes
      Set<List<RecordType>> actualRecordTypes =
          allArguments.stream()
              .map(recList -> recList.stream().map(Record::getRecordType).distinct().toList())
              .collect(Collectors.toSet());
      // we expect the actual invocation to be:
      Set<List<RecordType>> expected =
          Set.of(
              List.of(RecordType.valueOf("thing")),
              List.of(RecordType.valueOf("item")),
              List.of(RecordType.valueOf("widget")));
      assertEquals(expected, actualRecordTypes);
    }
  }

  // TODO AJ-1227: assess
  // Given the test file "four_tables.avro", does batchWritePfbStream make the right calls?
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
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      batchWriteService.batchWritePfbStream(dataStream, INSTANCE, Optional.of("id"));
      // TODO AJ-1227: what and how exactly should i be verifying here?
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
      fail(); // TODO AJ-1227: failure message?
    }
  }

  // TODO AJ-1227: assess
  @Test
  void testWritePfbStreamReturnsRecordCounts() {
    URL url = getClass().getResource("/four_tables.avro");
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWriteService.batchWritePfbStream(dataStream, INSTANCE, Optional.of("id"));
      RecordType expectedRecordType = RecordType.valueOf("data_release");
      RecordType expectedRecordType2 = RecordType.valueOf("submitted_aligned_reads");
      assertEquals(3, result.getUpdatedCount(expectedRecordType));
      assertEquals(1, result.getUpdatedCount(expectedRecordType2));
    } catch (IOException e) {
      fail(); // TODO AJ-1227: failure message?
    }
  }
}
