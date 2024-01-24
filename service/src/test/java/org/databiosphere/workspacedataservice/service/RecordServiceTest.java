package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles(profiles = {"mock-sam"})
class RecordServiceTest {

  @Autowired DataTypeInferer inferer;
  @Autowired InstanceService instanceService;
  @Autowired RecordDao recordDao;

  private UUID instanceId;

  @BeforeEach
  void beforeEach() {
    instanceId = UUID.randomUUID();
    instanceService.createInstance(instanceId, "v0.2");
  }

  @AfterEach
  void afterEach() {
    instanceService.deleteInstance(instanceId, "v0.2");
  }

  @Test
  void schemaChangesIncrementMetricsCounter() {
    // in-memory meter registry for unit-testing
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // create record service that uses the simple meter registry
    RecordService recordService = new RecordService(recordDao, inferer, meterRegistry);

    // assert the meter registry has no counters yet
    assertEquals(0, meterRegistry.getMeters().size());

    // insert a simple record; this will create "myAttr" as numeric
    RecordType recordType = RecordType.valueOf("myType");
    recordService.upsertSingleRecord(
        instanceId,
        recordType,
        "111",
        Optional.of("pk"),
        new RecordRequest(new RecordAttributes(Map.of("myAttr", BigDecimal.valueOf(123)))));

    // verify schema
    assertEquals(
        Map.of("pk", DataTypeMapping.STRING, "myAttr", DataTypeMapping.NUMBER),
        recordDao.getExistingTableSchema(instanceId, recordType));

    // assert the meter registry still has no counters; we only create the counter when altering
    // a column, and we just created a table but didn't issue any alters
    assertEquals(0, meterRegistry.getMeters().size());

    // insert another record, which will update the "myAttr" to be a string
    recordService.upsertSingleRecord(
        instanceId,
        recordType,
        "111",
        Optional.of("pk"),
        new RecordRequest(new RecordAttributes(Map.of("myAttr", "I am a string"))));

    // verify schema
    assertEquals(
        Map.of("pk", DataTypeMapping.STRING, "myAttr", DataTypeMapping.STRING),
        recordDao.getExistingTableSchema(instanceId, recordType));

    // we should have created a counter
    assertEquals(1, meterRegistry.getMeters().size());
    // get that counter
    Counter counter = assertInstanceOf(Counter.class, meterRegistry.getMeters().get(0));
    // assert the counter has been incremented once
    assertEquals(1, counter.count());
    // validate the tags for that counter
    assertEquals(recordType.getName(), counter.getId().getTag("RecordType"));
    assertEquals("myAttr", counter.getId().getTag("AttributeName"));
    assertEquals(instanceId.toString(), counter.getId().getTag("Instance"));
    assertEquals(DataTypeMapping.NUMBER.toString(), counter.getId().getTag("OldDataType"));
    assertEquals(DataTypeMapping.STRING.toString(), counter.getId().getTag("NewDataType"));
  }
}
