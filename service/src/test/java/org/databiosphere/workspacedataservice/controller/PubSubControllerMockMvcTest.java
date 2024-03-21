package org.databiosphere.workspacedataservice.controller;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.pubsub.PubSubMessage;
import org.databiosphere.workspacedataservice.pubsub.PubSubRequest;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(
    inheritProfiles = false,
    profiles = {"control-plane", "mock-sam"})
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false"
    })
class PubSubControllerMockMvcTest extends MockMvcTestBase {
  @MockBean private JobDao jobDao;
  @MockBean private CollectionDao collectionDao;

  @Test
  void statusUpdateNotification() throws Exception {
    // Arrange
    UUID jobId = UUID.randomUUID();

    // Job exists
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expected =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            collectionId.id(),
            GenericJobServerModel.StatusEnum.RUNNING,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.getJob(jobId)).thenReturn(expected);

    // Collection does not exist, so virtual collection is used
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    PubSubRequest request =
        new PubSubRequest(
            new PubSubMessage(
                Map.of(
                    "importId", jobId.toString(),
                    "currentStatus", "Upserting",
                    "newStatus", "Done"),
                null,
                "123456789",
                "2024-03-20T10:00:00.000Z"),
            null,
            "import-service-notify-test");

    // Act/Assert
    mockMvc
        .perform(
            post("/pubsub/import-status")
                .content(toJson(request))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andReturn();

    // Assert
    verify(jobDao, times(1)).succeeded(jobId);
  }
}
