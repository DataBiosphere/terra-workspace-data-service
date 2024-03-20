package org.databiosphere.workspacedataservice.controller;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.RawlsSnapshotSupportFactory;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupportFactory;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.pubsub.PubSubMessage;
import org.databiosphere.workspacedataservice.pubsub.PubSubRequest;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.recordsink.RawlsRecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(profiles = {"control-plane", "mock-sam"})
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false"
    })
class PubSubControllerMockMvcTest extends MockMvcTestBase {
  @MockBean private JobDao jobDao;
  @MockBean private CollectionDao collectionDao;

  // Since we're running with both control-plane and data-plane profiles simultaneously, Spring
  // does not know what to do with beans which are satisfied by two different mutually exclusive
  // implementations. In this @TestConfiguration, we explicitly specify @Primary beans for any
  // conflicts.
  @TestConfiguration
  static class SpecifyConflictingBeans {
    @Primary
    @Bean("overrideRecordSinkFactory")
    RecordSinkFactory overrideRecordSinkFactory(
        ObjectMapper mapper, GcsStorage storage, PubSub pubSub) {
      return new RawlsRecordSinkFactory(mapper, storage, pubSub);
    }

    @Primary
    @Bean("overrideSnapshotSupportFactory")
    SnapshotSupportFactory overrideSnapshotSupportFactory(
        RestClientRetry restClientRetry, ActivityLogger activityLogger, RawlsClient rawlsClient) {
      return new RawlsSnapshotSupportFactory(restClientRetry, activityLogger, rawlsClient);
    }
  }

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
                    "import_id", jobId.toString(),
                    "current_status", "Upserting",
                    "new_status", "Done"),
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
