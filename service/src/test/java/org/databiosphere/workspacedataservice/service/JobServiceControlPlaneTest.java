package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam", "control-plane"})
@SpringBootTest
class JobServiceControlPlaneTest {

  @Autowired JobService jobService;
  @Autowired TwdsProperties twdsProperties;

  @MockBean JobDao jobDao;
  @MockBean SamDao samDao;
  @MockBean CollectionDao collectionDao;

  @BeforeEach
  void beforeEach() {
    reset(jobDao);
    reset(collectionDao);
    reset(samDao);
  }

  /** requested job does not exist */
  @Test
  void jobDoesNotExist() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    // job not found
    when(jobDao.getJob(jobId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    // Act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).startsWith("Job");
  }

  /** requested job exists; its collection also exists. In the control plane, this is an error */
  @Test
  void collectionExists() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job exists
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(workspaceId.toString())).thenReturn(true);

    // Act / assert
    Exception actual = assertThrows(CollectionException.class, () -> jobService.getJob(jobId));

    // Assert
    assertEquals("Expected a virtual collection", actual.getMessage());
  }

  /** requested job exists; its collection is virtual and the user has permission */
  @Test
  void virtualCollectionWithPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    when(samDao.hasReadWorkspacePermission(collectionId.toString())).thenReturn(true);

    // Act
    GenericJobServerModel actual = jobService.getJob(jobId);

    // Assert
    assertEquals(expectedJob, actual);
  }

  /** requested job exists; its collection is virtual and the user does not have permission */
  @Test
  void virtualCollectionWithoutPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    when(samDao.hasReadWorkspacePermission(collectionId.toString())).thenReturn(false);

    // Act / assert
    Exception actual = assertThrows(AuthorizationException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).endsWith("this job.\"");
  }

  private GenericJobServerModel makeJob(UUID jobId, CollectionId collectionId) {
    return new GenericJobServerModel(
        jobId,
        GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
        collectionId.id(),
        GenericJobServerModel.StatusEnum.RUNNING,
        // set created and updated to now, but in UTC because that's how Postgres stores it
        OffsetDateTime.now(ZoneId.of("Z")),
        OffsetDateTime.now(ZoneId.of("Z")));
  }
}
