package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.ID_FIELD;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.pfb.PfbReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSource;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@TestPropertySource(properties = {"twds.write.batch.size=2"})
class BatchWriteServiceTest extends TestBase {

  @Autowired private RecordSourceFactory recordSourceFactory;
  @Autowired private RecordSinkFactory recordSinkFactory;
  @Autowired private BatchWriteService batchWriteService;
  @Autowired private CollectionDao collectionDao;
  @MockBean RecordDao recordDao;
  @SpyBean DataTypeInferer inferer;
  @SpyBean RecordService recordService;

  private static final UUID COLLECTION = UUID.fromString("aaaabbbb-cccc-dddd-1111-222233334444");
  private static final RecordType THING_TYPE = RecordType.valueOf("thing");

  @BeforeEach
  void setUp() {
    if (!collectionDao.collectionSchemaExists(COLLECTION)) {
      collectionDao.createSchema(COLLECTION);
    }
  }

  @AfterEach
  void tearDown() {
    collectionDao.dropSchema(COLLECTION);
  }

  @Test
  void testRejectsDuplicateKeys() throws IOException {
    String streamContents =
        "[{\"operation\": \"upsert\", \"record\": {\"id\": \"1\", \"type\": \"thing\", \"attributes\": {\"key\": \"value1\", \"key\": \"value2\"}}}]";
    InputStream is = new ByteArrayInputStream(streamContents.getBytes());

    RecordSource recordSource = recordSourceFactory.forJson(is);
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(CollectionId.of(COLLECTION))) {
      Exception ex =
          assertThrows(
              BadStreamingWriteRequestException.class,
              () -> batchWriteService.batchWrite(recordSource, recordSink, THING_TYPE, RECORD_ID));
      String errorMessage = ex.getMessage();
      assertEquals("Duplicate field 'key'", errorMessage);
    }
  }

  // Do we get the right counts back when importing from a mock PFB stream?
  @Test
  void batchWriteResultCountsFromMock() {
    // create a stream with 5 "things", 10 "items", and 15 "widgets"
    Map<String, Integer> counts = Map.of("thing", 5, "item", 10, "widget", 15);
    try (DataFileStream<GenericRecord> pfbStream = PfbTestUtils.mockPfbStream(counts)) {
      BatchWriteResult result =
          batchWritePfbStream(pfbStream, /* primaryKey= */ ID_FIELD, ImportMode.BASE_ATTRIBUTES);
      assertEquals(3, result.entrySet().size());
      assertEquals(5, result.getUpdatedCount(RecordType.valueOf("thing")));
      assertEquals(10, result.getUpdatedCount(RecordType.valueOf("item")));
      assertEquals(15, result.getUpdatedCount(RecordType.valueOf("widget")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  // the test file "/four_rows.avro" contains 4 records - 1 "data_release" and 3
  // "submitted_aligned_reads". Do we get the right counts back when importing this file?
  @Test
  void batchWriteResultCountsFromPfb() {
    URL url = getClass().getResource("/avro/four_rows.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWritePfbStream(dataStream, /* primaryKey= */ ID_FIELD, ImportMode.BASE_ATTRIBUTES);

      assertEquals(2, result.entrySet().size());
      assertEquals(3, result.getUpdatedCount(RecordType.valueOf("data_release")));
      assertEquals(1, result.getUpdatedCount(RecordType.valueOf("submitted_aligned_reads")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  void batchWriteRelationsFromPfb() {
    URL url = getClass().getResource("/avro/test.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWritePfbStream(dataStream, /* primaryKey= */ ID_FIELD, ImportMode.RELATIONS);

      assertEquals(1, result.entrySet().size());
      // The 'files' record type has relations, so it should have been updated
      assertEquals(3202, result.getUpdatedCount(RecordType.valueOf("files")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  void batchWriteSomeRelationsFromPfb() {
    URL url = getClass().getResource("/avro/forward_relations.avro");
    assertNotNull(url);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      BatchWriteResult result =
          batchWritePfbStream(dataStream, /* primaryKey= */ ID_FIELD, ImportMode.RELATIONS);

      assertEquals(1, result.entrySet().size());
      // Only one of the data_release records had any relations present
      assertEquals(1, result.getUpdatedCount(RecordType.valueOf("data_release")));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private BatchWriteResult batchWritePfbStream(
      DataFileStream<GenericRecord> pfbStream, String primaryKey, ImportMode importMode)
      throws IOException {
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(CollectionId.of(COLLECTION))) {
      return batchWriteService.batchWrite(
          recordSourceFactory.forPfb(pfbStream, importMode),
          recordSink,
          /* recordType= */ null,
          primaryKey);
    }
  }
}
