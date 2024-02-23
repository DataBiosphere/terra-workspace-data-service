package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(profiles = {"control-plane"})
@SpringBootTest
@TestPropertySource(
    properties = {
      // TODO(AJ-1656): control-plane should not require instance config in any form, this is a hold
      //   over from direct injection of @Value('twds.instance.workspace-id')
      "twds.instance.workspace-id=",
    })
class JobServiceControlPlaneTest extends JobServiceBaseTest {
  @Autowired JobService jobService;
  @MockBean JobDao jobDao;
  @MockBean SamDao samDao;

  @BeforeEach
  void beforeEach() {
    reset(jobDao);
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

  /** requested job exists; its collection is virtual and the user has permission */
  @Test
  void virtualCollectionWithPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
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
    // user has permission to the workspace with the same id as the collection
    when(samDao.hasReadWorkspacePermission(collectionId.toString())).thenReturn(false);

    // Act / assert
    AuthenticationMaskableException actual =
        assertThrows(AuthenticationMaskableException.class, () -> jobService.getJob(jobId));

    // Assert
    assertEquals("Job", actual.getObjectType());
  }

  // ==================================================
  // ========== tests for getJobsForCollection ========
  // ==================================================

  /** Collection is virtual; user has permission */
  @Test
  void listJobsVirtualCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // user has permission to the workspace with the same id as the collection
    when(samDao.hasReadWorkspacePermission(collectionId.toString())).thenReturn(true);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 2));

    // Act
    List<GenericJobServerModel> actual = jobService.getJobsForCollection(collectionId, allStatuses);

    // Assert
    // this is verifying permissions only; only smoke-testing correctness of the result
    assertThat(actual).hasSize(2);
  }

  /** Collection is virtual; user does not have permission */
  @Test
  void listJobsVirtualCollectionNoPermission() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // user has permission to the workspace with the same id as the collection
    when(samDao.hasReadWorkspacePermission(collectionId.toString())).thenReturn(false);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 3));

    // Act / assert
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> jobService.getJobsForCollection(collectionId, allStatuses));

    // Assert
    assertEquals("Collection", actual.getObjectType());
  }
}
