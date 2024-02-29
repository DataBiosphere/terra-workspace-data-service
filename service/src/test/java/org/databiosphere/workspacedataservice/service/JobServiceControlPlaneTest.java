package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
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

@ActiveProfiles(profiles = {"control-plane"})
@SpringBootTest
class JobServiceControlPlaneTest extends JobServiceBaseTest {

  @Autowired JobService jobService;
  @MockBean JobDao jobDao;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockBean CollectionDao collectionDao;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  @BeforeEach
  void beforeEach() {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(any(WorkspaceId.class)))
        .thenReturn(samAuthorizationDao);
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
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(true);

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
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(true);

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
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(false);

    // Act / assert
    AuthenticationMaskableException actual =
        assertThrows(AuthenticationMaskableException.class, () -> jobService.getJob(jobId));

    // Assert
    assertEquals("Job", actual.getObjectType());
  }

  // ==================================================
  // ========== tests for getJobsForCollection ========
  // ==================================================

  /** Collection exists. In the control plane, this is an error */
  @Test
  void listJobsCollectionExists() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // collection for this job exists
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // user has permission to that workspace
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(true);

    // Act / assert
    Exception actual =
        assertThrows(
            CollectionException.class,
            () -> jobService.getJobsForCollection(collectionId, Optional.of(allStatuses)));

    // Assert
    assertEquals("Expected a virtual collection", actual.getMessage());
  }

  /** Collection is virtual; user has permission */
  @Test
  void listJobsVirtualCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(true);
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

  /** Collection is virtual; user does not have permission */
  @Test
  void listJobsVirtualCollectionNoPermission() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to the workspace with the same id as the collection
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(false);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 3));

    // Act / assert
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> jobService.getJobsForCollection(collectionId, Optional.of(allStatuses)));

    // Assert
    assertEquals("Collection", actual.getObjectType());
  }
}
