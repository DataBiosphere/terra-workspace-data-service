package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class BatchWriteServiceTest {

  @Autowired private BatchWriteService batchWriteService;
  @Autowired private InstanceDao instanceDao;

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
  void testRejectsDuplicateKeys() throws IOException {
    String streamContents =
        "[{\"operation\": \"upsert\", \"record\": {\"id\": \"1\", \"type\": \"thing\", \"attributes\": {\"key\": \"value1\", \"key\": \"value2\"}}}]";
    InputStream is = new ByteArrayInputStream(streamContents.getBytes());

    Exception ex =
        assertThrows(
            BadStreamingWriteRequestException.class,
            () ->
                batchWriteService.batchWriteJsonStream(
                    is, INSTANCE, THING_TYPE, java.util.Optional.<String>empty()));

    String errorMessage = ex.getMessage();
    assertEquals(errorMessage, "Duplicate field 'key'");
  }
}
