package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
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
  @MockBean CollectionService collectionService;

  /* test cases
       - getJob
         + job does not exist (404)
         - job exists
           - collection exists
             - job collection = $WORKSPACE_ID, user has permission to $WORKSPACE_ID (200)
             - job collection = $WORKSPACE_ID, user does not have permission to $WORKSPACE_ID (404)
             - job collection != $WORKSPACE_ID (exception)
           - collection does not exist
             - user has permission to workspace where workspace == collection (200)
             - user does not have permission to workspace where workspace == collection (404)
  */

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

  @Test
  void defaultCollectionWithPermission() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);
    // collection for this job exists and is associated with the $WORKSPACE_ID workspace
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(getEnvWorkspaceId());
    // user has permission to that workspace
    when(collectionService.canReadCollection(collectionId)).thenReturn(true);

    // Act
    GenericJobServerModel actual = jobService.getJob(jobId);
    // Success
    assertThat(actual).isEqualTo(expectedJob);
  }

  //  @Test
  //  void returnJobStatusWithPermission() throws ApiException {
  //    // Make sure user has permission
  //    given(mockSamResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
  //        .willReturn(true);
  //    // Tell jobDao to return
  //    UUID collectionId = UUID.randomUUID();
  //    UUID jobId = UUID.randomUUID();
  //    GenericJobServerModel expectedJob =
  //        new GenericJobServerModel(
  //            jobId,
  //            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
  //            collectionId,
  //            GenericJobServerModel.StatusEnum.RUNNING,
  //            // set created and updated to now, but in UTC because that's how Postgres stores it
  //            OffsetDateTime.now(ZoneId.of("Z")),
  //            OffsetDateTime.now(ZoneId.of("Z")));
  //    given(jobDao.getJob(jobId)).willReturn(expectedJob);
  //    // Ask jobService to get it
  //    GenericJobServerModel fetchedJob = jobService.getJob(jobId);
  //    // Success
  //    assertThat(expectedJob).isEqualTo(fetchedJob);
  //  }
  //
  //  @Test
  //  void doNotReturnWithoutPermission() throws ApiException {
  //    // Make sure user does not have permission
  //    given(mockSamResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
  //        .willReturn(false);
  //    // Tell jobDao to return
  //    UUID collectionId = UUID.randomUUID();
  //    UUID jobId = UUID.randomUUID();
  //    GenericJobServerModel expectedJob =
  //        new GenericJobServerModel(
  //            jobId,
  //            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
  //            collectionId,
  //            GenericJobServerModel.StatusEnum.RUNNING,
  //            // set created and updated to now, but in UTC because that's how Postgres stores it
  //            OffsetDateTime.now(ZoneId.of("Z")),
  //            OffsetDateTime.now(ZoneId.of("Z")));
  //
  //    given(jobDao.getJob(any())).willReturn(expectedJob);
  //    // Ask jobService to get it
  //    assertThrows(AuthorizationException.class, () -> jobService.getJob(jobId));
  //  }

  private GenericJobServerModel makeJob(UUID jobId, CollectionId collectionId) {
    GenericJobServerModel expectedJob =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            collectionId.id(),
            GenericJobServerModel.StatusEnum.RUNNING,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));

    return expectedJob;
  }

  private WorkspaceId getEnvWorkspaceId() {
    return WorkspaceId.of(twdsProperties.getInstance().getWorkspaceUuid());
  }
}
