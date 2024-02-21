package org.databiosphere.workspacedataservice.service;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordService.METRIC_COL_CHANGE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_ATTRIBUTE_NAME;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_COLLECTION;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_NEW_DATATYPE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_OLD_DATATYPE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_RECORD_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.observability.TestObservationRegistryConfig;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles(profiles = {"mock-sam"})
@Import(TestObservationRegistryConfig.class)
class RecordServiceTest extends TestBase {

  @Autowired DataTypeInferer inferer;
  @Autowired CollectionService collectionService;
  @Autowired RecordDao recordDao;
  // overridden by TestObservationRegistryConfig with a TestObservationRegistry
  @Autowired private ObservationRegistry observationRegistry;

  private UUID collectionId;

  @BeforeEach
  void beforeEach() {
    collectionId = UUID.randomUUID();
    collectionService.createCollection(collectionId, "v0.2");
  }

  @AfterEach
  void afterEach() {
    collectionService.deleteCollection(collectionId, "v0.2");
  }

  @Test
  void schemaChangesIncrementMetricsCounter() {
    TestObservationRegistry testObservationRegistry =
        assertInstanceOf(TestObservationRegistry.class, observationRegistry);
    // create record service that uses the simple meter registry
    RecordService recordService = new RecordService(recordDao, inferer, testObservationRegistry);

    // assert the observation registry has no observations
    assertThat(testObservationRegistry).hasNumberOfObservationsEqualTo(0);

    // insert a simple record; this will create "myAttr" as numeric
    RecordType recordType = RecordType.valueOf("myType");
    recordService.upsertSingleRecord(
        collectionId,
        recordType,
        "111",
        Optional.of("pk"),
        new RecordRequest(new RecordAttributes(Map.of("myAttr", BigDecimal.valueOf(123)))));

    // verify schema
    assertEquals(
        Map.of("pk", DataTypeMapping.STRING, "myAttr", DataTypeMapping.NUMBER),
        recordDao.getExistingTableSchema(collectionId, recordType));

    // assert the observation registry still has no counters; we only create an observation when
    // altering a column, and we just created a table but didn't issue any alters
    assertThat(testObservationRegistry).hasNumberOfObservationsEqualTo(0);

    // insert another record, which will update the "myAttr" to be a string
    recordService.upsertSingleRecord(
        collectionId,
        recordType,
        "111",
        Optional.of("pk"),
        new RecordRequest(new RecordAttributes(Map.of("myAttr", "I am a string"))));

    // verify schema
    assertEquals(
        Map.of("pk", DataTypeMapping.STRING, "myAttr", DataTypeMapping.STRING),
        recordDao.getExistingTableSchema(collectionId, recordType));

    // we should have created an observation
    assertThat(testObservationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsWithNameEqualTo(METRIC_COL_CHANGE, 1)
        .hasObservationWithNameEqualTo(METRIC_COL_CHANGE)
        .that()
        .hasLowCardinalityKeyValue(TAG_OLD_DATATYPE, DataTypeMapping.NUMBER.toString())
        .hasLowCardinalityKeyValue(TAG_NEW_DATATYPE, DataTypeMapping.STRING.toString())
        .hasHighCardinalityKeyValue(TAG_RECORD_TYPE, recordType.getName())
        .hasHighCardinalityKeyValue(TAG_ATTRIBUTE_NAME, "myAttr")
        .hasHighCardinalityKeyValue(TAG_COLLECTION, collectionId.toString())
        .hasBeenStarted()
        .hasBeenStopped();
  }
}
