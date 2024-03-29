package org.databiosphere.workspacedataservice.service;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(
    profiles = {
      "data-plane", // need _a_ profile even if this test only contains tests for common behavior
    })
@DirtiesContext
@SpringBootTest(properties = {"twds.instance.workspace-id=f01dab1e-0000-1111-2222-000011112222"})
public class JobServiceTest extends JobServiceTestBase {
  @Autowired JobService jobService;
  @MockBean JobDao jobDao;

  @Test
  void processJobStatusUpdateSuccess() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest();

    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, times(1)).succeeded(jobId);
  }

  @Test
  void processJobStatusUpdateError() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest();

    JobStatusUpdate update =
        new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.ERROR, "Something went wrong");

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, times(1)).fail(jobId, "Something went wrong");
  }

  @Test
  void processJobStatusUpdateNoop() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest();

    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.RUNNING);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, never()).running(jobId);
  }

  @Test
  void processJobStatusUpdateForTerminalJob() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest(StatusEnum.SUCCEEDED);

    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.SUCCEEDED, StatusEnum.RUNNING);

    // Act/Assert
    ValidationException e =
        assertThrows(ValidationException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    assertEquals("Unable to update terminal status for job %s".formatted(jobId), e.getMessage());
    verify(jobDao, never()).succeeded(jobId);
  }

  @Test
  void processJobStatusUpdateForNonExistentJob() {
    // Arrange
    UUID jobId = randomUUID();
    when(jobDao.getJob(jobId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act/Assert
    assertThrows(MissingObjectException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    verify(jobDao, never()).succeeded(jobId);
  }

  private UUID setupProcessJobStatusUpdateTest(StatusEnum initialStatus) {
    UUID jobId = randomUUID();
    // Job exists
    GenericJobServerModel expectedJob =
        new GenericJobServerModel(
            jobId,
            JobTypeEnum.DATA_IMPORT,
            randomUUID(),
            initialStatus,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);

    return jobId;
  }

  private UUID setupProcessJobStatusUpdateTest() {
    return setupProcessJobStatusUpdateTest(StatusEnum.RUNNING);
  }
}
