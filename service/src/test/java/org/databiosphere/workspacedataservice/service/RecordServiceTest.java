package org.databiosphere.workspacedataservice.service;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordService.METRIC_COL_CHANGE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_ATTRIBUTE_NAME;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_COLLECTION;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_NEW_DATATYPE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_OLD_DATATYPE;
import static org.databiosphere.workspacedataservice.service.RecordService.TAG_RECORD_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.observation.tck.TestObservationRegistry;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.annotations.WithTestObservationRegistry;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@WithTestObservationRegistry
class RecordServiceTest extends ControlPlaneTestBase {

  @Autowired DataTypeInferer inferer;
  @Autowired CollectionService collectionService;
  @Autowired RecordDao recordDao;
  @Autowired TestObservationRegistry observationRegistry;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired WorkspaceRepository workspaceRepository;

  private UUID collectionId;

  @BeforeEach
  void beforeEach() {
    // save a WDS-powered workspace
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));
    // create a collection in that workspace
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, workspaceId);
    collectionId = collectionServerModel.getId();
  }

  @AfterEach
  void afterEach() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @Test
  void schemaChangesIncrementMetricsCounter() {
    // create record service that uses the simple meter registry
    RecordService recordService = new RecordService(recordDao, inferer, observationRegistry);

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
    assertThat(observationRegistry)
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
