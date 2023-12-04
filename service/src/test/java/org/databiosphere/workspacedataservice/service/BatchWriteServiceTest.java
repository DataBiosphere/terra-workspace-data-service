package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter.ID_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.TreeMap;
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
  // table schema only once even if we insert multiple batches.
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

  // when batchWritePfbStream is called and the input contains multiple record types, we should
  // infer each type's table schema only once even if we insert multiple batches.
  @Test
  void schemaInferredOnceForEachRecordType() throws IOException {

    Optional<String> primaryKey = Optional.of(ID_FIELD);

    // create a stream with 5 "things", 10 "items", and 15 "widgets"
    // use a TreeMap, so we can control the order in which the record types appear in the stream.
    Map<String, Integer> counts = new TreeMap<>();
    counts.put("thing", 5);
    counts.put("item", 10);
    counts.put("widget", 15);
    try (DataFileStream<GenericRecord> pfbStream = PfbTestUtils.mockPfbStream(counts)) {

      // call the BatchWriteService. Since this test specifies a batch size of 2, it will result
      // in the following calls:
      // * batchUpsertWithErrorCapture 3 times for "thing"
      // * batchUpsertWithErrorCapture 6 times for "item"
      //    - 1 call for batch #3 which will be (thing, item)
      //    - 5 more calls for batches #4-7 which will be (item, item)
      //    - 1 call for batch #8 which will be (item, widget)
      // * batchUpsertWithErrorCapture 8 times for "widget"
      //    - 1 call for batch #8 which will be (item, widget)
      //    - 7 more calls for batches #9-15 which will be (widget, widget)
      // * inferTypes once for each of "thing", "item", and "widget"
      batchWriteService.batchWritePfbStream(
          pfbStream,
          INSTANCE,
          Optional.of(ID_FIELD),
          PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);

      // verify calls to batchUpsertWithErrorCapture
      verify(recordService, times(3))
          .batchUpsertWithErrorCapture(
              eq(INSTANCE), eq(RecordType.valueOf("thing")), any(), any(), eq(primaryKey.get()));
      verify(recordService, times(5))
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
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  // Do we get the right counts back when importing from a mock PFB stream?
  @Test
  void batchWriteResultCountsFromMock() throws IOException {
    // create a stream with 5 "things", 10 "items", and 15 "widgets"
    Map<String, Integer> counts = Map.of("thing", 5, "item", 10, "widget", 15);
    try (DataFileStream<GenericRecord> pfbStream = PfbTestUtils.mockPfbStream(counts)) {
      BatchWriteResult result =
          batchWriteService.batchWritePfbStream(
              pfbStream,
              INSTANCE,
              Optional.of(ID_FIELD),
              PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);
      assertEquals(3, result.entrySet().size());
      assertEquals(5, result.getUpdatedCount(RecordType.valueOf("thing")));
      assertEquals(10, result.getUpdatedCount(RecordType.valueOf("item")));
      assertEquals(15, result.getUpdatedCount(RecordType.valueOf("widget")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  // the test file "/four_tables.avro" contains 4 records - 1 "data_release" and 3
  // "submitted_aligned_reads". Do we get the right counts back when importing this file?
  @Test
  void batchWriteResultCountsFromPfb() {
    URL url = getClass().getResource("/four_tables.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWriteService.batchWritePfbStream(
              dataStream,
              INSTANCE,
              Optional.of(ID_FIELD),
              PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);

      assertEquals(2, result.entrySet().size());
      assertEquals(3, result.getUpdatedCount(RecordType.valueOf("data_release")));
      assertEquals(1, result.getUpdatedCount(RecordType.valueOf("submitted_aligned_reads")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  void batchWriteRelationsFromPfb() {
    URL url = getClass().getResource("/test.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWriteService.batchWritePfbStream(
              dataStream,
              INSTANCE,
              Optional.of(ID_FIELD),
              PfbStreamWriteHandler.PfbImportMode.RELATIONS);

      assertEquals(1, result.entrySet().size());
      // The 'files' record type has relations, so it should have been updated
      assertEquals(3202, result.getUpdatedCount(RecordType.valueOf("files")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  void batchWriteSomeRelationsFromPfb() {
    URL url = getClass().getResource("/forward_relations.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWriteService.batchWritePfbStream(
              dataStream,
              INSTANCE,
              Optional.of(ID_FIELD),
              PfbStreamWriteHandler.PfbImportMode.RELATIONS);

      assertEquals(1, result.entrySet().size());
      // Only one of the data_release records had any relations present
      assertEquals(1, result.getUpdatedCount(RecordType.valueOf("data_release")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
