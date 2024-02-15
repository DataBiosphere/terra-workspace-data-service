package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"data-plane"})
@SpringBootTest(properties = {"twds.instance.workspace-id=f01dab1e-0000-1111-2222-000011112222"})
class JobServiceDataPlaneTest extends JobServiceBaseTest {

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

  // ==================================================
  // ========== tests for getJob ======================
  // ==================================================

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

  /** requested job exists; its collection uses the default workspace and user has permission */
  @Test
  void defaultCollectionWithPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job exists and is associated with the $WORKSPACE_ID workspace
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(getEnvWorkspaceId());
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(getEnvWorkspaceId().toString())).thenReturn(true);

    // Act
    GenericJobServerModel actual = jobService.getJob(jobId);

    // Assert
    assertThat(actual).isEqualTo(expectedJob);
  }

  /** requested job exists; its collection uses the default workspace but user has no permission */
  @Test
  void defaultCollectionWithoutPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job exists and is associated with the $WORKSPACE_ID workspace
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(getEnvWorkspaceId());
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(getEnvWorkspaceId().toString())).thenReturn(false);

    // Act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).startsWith("Job");
  }

  /** requested job exists; its collection does not use the default workspace */
  @Test
  void nonDefaultCollection() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job exists, but is associated with a workspace other than the
    // $WORKSPACE_ID workspace
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(workspaceId.toString())).thenReturn(true);

    // Act / assert
    Exception actual = assertThrows(CollectionException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).startsWith("Found unexpected workspaceId for collection");
  }

  /**
   * requested job exists; its collection does not exist. This test relies on the "data-plane"
   * Spring profile setting twds.tenancy.allow-virtual-collections=false.
   */
  @Test
  @Disabled("AJ-1630") // TODO AJ-1630
  void collectionDoesNotExist() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(workspaceId.toString())).thenReturn(true);

    // Act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // Assert
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  // ==================================================
  // ========== tests for getJobsForCollection ========
  // ==================================================

  /** Collection does not exist */
  @Test
  @Disabled("AJ-1630") // TODO AJ-1630
  void listJobsCollectionDoesNotExist() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection for this job does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    // Act / assert
    Exception actual =
        assertThrows(
            MissingObjectException.class,
            () -> jobService.getJobsForCollection(collectionId, allStatuses));

    // Assert
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** Collection exists, associated with default workspace, user has permission */
  @Test
  void listJobsDefaultCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection exists and is associated with the $WORKSPACE_ID workspace
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(getEnvWorkspaceId());
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(getEnvWorkspaceId().toString())).thenReturn(true);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 2));

    // Act
    List<GenericJobServerModel> actual = jobService.getJobsForCollection(collectionId, allStatuses);

    // Assert
    // this is verifying permissions only; only smoke-testing correctness of the result
    assertThat(actual).hasSize(2);
  }

  /** Collection exists, associated with default workspace, user does not have permission */
  @Test
  void listJobsDefaultCollectionNoPermission() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection exists and is associated with the $WORKSPACE_ID workspace
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(getEnvWorkspaceId());
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(getEnvWorkspaceId().toString())).thenReturn(false);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 3));

    // Act / assert
    Exception actual =
        assertThrows(
            MissingObjectException.class,
            () -> jobService.getJobsForCollection(collectionId, allStatuses));

    // Assert
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** Collection exists, associated with a non-default workspace */
  @Test
  void listJobsNonDefaultCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // collection exists and is associated with a non-default workspace
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // user has permission to that workspace
    when(samDao.hasReadWorkspacePermission(workspaceId.toString())).thenReturn(true);
    // return some jobs when listing this collection
    when(jobDao.getJobsForCollection(eq(collectionId), any()))
        .thenReturn(makeJobList(collectionId, 4));

    // Act / assert
    Exception actual =
        assertThrows(
            CollectionException.class,
            () -> jobService.getJobsForCollection(collectionId, allStatuses));

    // Assert
    assertThat(actual.getMessage()).startsWith("Found unexpected workspaceId for collection");
  }

  // ==================================================
  // ========== helpers ===============================
  // ==================================================

  private WorkspaceId getEnvWorkspaceId() {
    return WorkspaceId.of(twdsProperties.getInstance().getWorkspaceUuid());
  }
}
