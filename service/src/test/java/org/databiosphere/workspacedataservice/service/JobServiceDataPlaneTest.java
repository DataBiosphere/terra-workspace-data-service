package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam"})
@SpringBootTest
class JobServiceDataPlaneTest {
  @MockBean JobDao jobDao;
  @Autowired SamDao samDao;
  @Autowired JobService jobService;
  @MockBean SamClientFactory mockSamClientFactory;

  ResourcesApi mockSamResourcesApi = Mockito.mock(ResourcesApi.class);

  @BeforeEach
  void setUp() {
    given(mockSamClientFactory.getResourcesApi(null)).willReturn(mockSamResourcesApi);
  }

  @Test
  void returnJobStatusWithPermission() throws ApiException {
    // Make sure user has permission
    given(mockSamResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(true);
    // Tell jobDao to return
    UUID collectionId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    GenericJobServerModel expectedJob =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            collectionId,
            GenericJobServerModel.StatusEnum.RUNNING,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    given(jobDao.getJob(jobId)).willReturn(expectedJob);
    // Ask jobService to get it
    GenericJobServerModel fetchedJob = jobService.getJob(jobId);
    // Success
    assertThat(expectedJob).isEqualTo(fetchedJob);
  }

  @Test
  void doNotReturnWithoutPermission() throws ApiException {
    // Make sure user does not have permission
    given(mockSamResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(false);
    // Tell jobDao to return
    UUID collectionId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    GenericJobServerModel expectedJob =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            collectionId,
            GenericJobServerModel.StatusEnum.RUNNING,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));

    given(jobDao.getJob(any())).willReturn(expectedJob);
    // Ask jobService to get it
    assertThrows(AuthorizationException.class, () -> jobService.getJob(jobId));
  }
}
