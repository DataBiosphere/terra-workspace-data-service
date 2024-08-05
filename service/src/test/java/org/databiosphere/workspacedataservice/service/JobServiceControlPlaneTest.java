package org.databiosphere.workspacedataservice.service;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(profiles = {"control-plane"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
class JobServiceControlPlaneTest extends JobServiceTestBase {

  // TODO AJ-1660: remove samAuthorizationDaoFactory mock entirely

  @Autowired JobService jobService;
  @MockBean JobDao jobDao;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockBean CollectionDao collectionDao;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  /** requested job does not exist */
  @Test
  void jobDoesNotExist() {
    // Arrange
    UUID jobId = randomUUID();
    // job not found
    when(jobDao.getJob(jobId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    // Act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).startsWith("Job");
  }

  /** requested job exists; its collection is virtual and the user has permission */
  @Test
  void virtualCollectionWithPermission() {
    // Arrange
    UUID jobId = randomUUID();
    CollectionId collectionId = CollectionId.of(randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    stubReadWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(true);

    // Act
    GenericJobServerModel actual = jobService.getJob(jobId);

    // Assert
    assertEquals(expectedJob, actual);
  }

  // ==================================================
  // ========== tests for getJobsForCollection ========
  // ==================================================

  /** Collection is virtual; user has permission */
  @Test
  void listJobsVirtualCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(randomUUID());
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    stubReadWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(true);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 2));

    // Act
    List<GenericJobServerModel> actual =
        jobService.getJobsForCollection(collectionId, Optional.of(allStatuses));

    // Assert
    // this is verifying permissions only; only smoke-testing correctness of the result
    assertThat(actual).hasSize(2);
  }

  @Test
  void processJobStatusUpdateSuccess() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest();

    JobStatusUpdate update =
        new JobStatusUpdate(
            jobId,
            GenericJobServerModel.StatusEnum.RUNNING,
            GenericJobServerModel.StatusEnum.SUCCEEDED);

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
        new JobStatusUpdate(
            jobId,
            GenericJobServerModel.StatusEnum.RUNNING,
            GenericJobServerModel.StatusEnum.ERROR,
            "Something went wrong");

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, times(1)).fail(jobId, "Something went wrong");
  }

  @Test
  void processJobStatusUpdateNoop() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest();

    JobStatusUpdate update =
        new JobStatusUpdate(
            jobId,
            GenericJobServerModel.StatusEnum.RUNNING,
            GenericJobServerModel.StatusEnum.RUNNING);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, never()).running(jobId);
  }

  @Test
  void processJobStatusUpdateForTerminalJob() {
    // Arrange
    UUID jobId = setupProcessJobStatusUpdateTest(GenericJobServerModel.StatusEnum.SUCCEEDED);

    JobStatusUpdate update =
        new JobStatusUpdate(
            jobId,
            GenericJobServerModel.StatusEnum.SUCCEEDED,
            GenericJobServerModel.StatusEnum.RUNNING);

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

    JobStatusUpdate update =
        new JobStatusUpdate(
            jobId,
            GenericJobServerModel.StatusEnum.RUNNING,
            GenericJobServerModel.StatusEnum.SUCCEEDED);

    // Act/Assert
    assertThrows(MissingObjectException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    verify(jobDao, never()).succeeded(jobId);
  }

  private UUID setupProcessJobStatusUpdateTest(GenericJobServerModel.StatusEnum initialStatus) {
    UUID jobId = randomUUID();
    // Job exists
    GenericJobServerModel expectedJob =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            randomUUID(),
            initialStatus,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);

    return jobId;
  }

  private UUID setupProcessJobStatusUpdateTest() {
    return setupProcessJobStatusUpdateTest(GenericJobServerModel.StatusEnum.RUNNING);
  }

  private OngoingStubbing<Boolean> stubReadWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasReadWorkspacePermission());
  }
}
