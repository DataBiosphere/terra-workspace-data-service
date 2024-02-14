package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
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

@ActiveProfiles(profiles = {"mock-sam", "data-plane"})
@SpringBootTest(properties = {"twds.instance.workspace-id=f01dab1e-0000-1111-2222-000011112222"})
class JobServiceDataPlaneTest {

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

    // act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // assert
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
    // Success
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

    // act / assert
    Exception actual = assertThrows(AuthorizationException.class, () -> jobService.getJob(jobId));

    // Success
    assertThat(actual.getMessage()).endsWith("this job.\"");
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

    // act / assert
    Exception actual = assertThrows(CollectionException.class, () -> jobService.getJob(jobId));

    // Success
    assertThat(actual.getMessage()).startsWith("Found unexpected workspaceId for collection");
  }

  /** requested job exists; its collection does not exist */
  @Test
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

    // act / assert
    Exception actual = assertThrows(MissingObjectException.class, () -> jobService.getJob(jobId));

    // Success
    assertThat(actual.getMessage()).startsWith("Collection");
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

  private WorkspaceId getEnvWorkspaceId() {
    return WorkspaceId.of(twdsProperties.getInstance().getWorkspaceUuid());
  }
}
