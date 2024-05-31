package org.databiosphere.workspacedataservice.tsv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.recordsource.TsvRecordSource;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@TestPropertySource(properties = {"twds.write.batch.size=2"})
class TsvBatchTypeDetectionTest extends TestBase {

  @Autowired private RecordSourceFactory recordSourceFactory;
  @Autowired private RecordSinkFactory recordSinkFactory;
  @Autowired private BatchWriteService batchWriteService;
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private CollectionDao collectionDao;
  @Autowired RecordDao recordDao;
  @SpyBean DataTypeInferer inferer;
  @SpyBean RecordService recordService;

  private static final UUID COLLECTION = UUID.fromString("aaaabbbb-cccc-dddd-1111-222233334444");
  private static final RecordType THING_TYPE = RecordType.valueOf("thing");

  @BeforeEach
  void setUp() {
    if (!collectionDao.collectionSchemaExists(CollectionId.of(COLLECTION))) {
      collectionDao.createSchema(COLLECTION);
    }
  }

  @AfterEach
  void tearDown() {
    collectionDao.dropSchema(COLLECTION);
  }

  // when batchWriteTsvStream is called with a single specified RecordType, we should not fail if
  // the first batch and a later batch have conflicting data types
  @Test
  void schemaSafeAcrossBatches() throws IOException {
    // generate a TSV input with one column. The first batch of rows has numerics for this column,
    // but the second batch has a string. This should not fail the import.
    String tsvContent = """
id\tmyColumn
1\t1
2\t2
3\t"I'm a string!"
4\t4
5\t5
""";
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContent.getBytes());

    // other params
    String primaryKey = "id";

    // call the BatchWriteService. Since this test specifies a batch size of 2, the type detection
    // in the first batch will determine that myColumn is a number. The second batch will encounter
    // a string for that column. The third batch is all numbers again, but by then we've already
    // made the column a string.
    TsvRecordSource recordSource =
        recordSourceFactory.forTsv(file.getInputStream(), THING_TYPE, Optional.of(primaryKey));
    // batchWrite will fail if we are not correctly re-detecting datatypes in later batches
    // (note this is a try-with-resources; an exception from batchWrite() will still fail the test)
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(CollectionId.of(COLLECTION))) {
      batchWriteService.batchWrite(recordSource, recordSink, THING_TYPE, primaryKey);
    }

    // we should write three batches
    verify(recordService, times(3))
        .batchUpsert(eq(COLLECTION), eq(THING_TYPE), any(), any(), eq(primaryKey));

    // and we should have inferred the schema three times as well
    verify(inferer, times(3)).inferTypes(ArgumentMatchers.<List<Record>>any());

    // retrieve the final record schema
    RecordTypeSchema actualRecordSchema =
        recordOrchestratorService.describeRecordType(COLLECTION, "v0.2", THING_TYPE);
    AttributeSchema actual = actualRecordSchema.getAttributeSchema("myColumn");

    // "myColumn" should be a string, reflecting the change we saw in the second batch
    assertEquals(DataTypeMapping.STRING.name(), actual.datatype());
  }
}
