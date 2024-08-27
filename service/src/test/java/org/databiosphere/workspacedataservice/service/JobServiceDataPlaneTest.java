package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.common.TestBase.HARDCODED_WORKSPACE_ID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"data-plane"})
@DirtiesContext
@SpringBootTest(
    properties = {
      "twds.instance.workspace-id=" + HARDCODED_WORKSPACE_ID,
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
class JobServiceDataPlaneTest extends JobServiceTestBase {

  @Autowired JobService jobService;
  @Autowired TwdsProperties twdsProperties;

  @MockBean JobDao jobDao;
  @MockBean CollectionService collectionService;

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

  /** requested job exists; its collection uses the default workspace */
  @Test
  void defaultCollection() {
    // Arrange
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    GenericJobServerModel expectedJob = makeJob(jobId, collectionId);
    // job exists
    when(jobDao.getJob(jobId)).thenReturn(expectedJob);

    // CollectionService.validateCollection() does not throw

    // Act
    GenericJobServerModel actual = jobService.getJob(jobId);

    // Assert
    assertThat(actual).isEqualTo(expectedJob);
  }

  // ==================================================
  // ========== tests for getJobsForCollection ========
  // ==================================================

  /** Collection does not exist */
  @Test
  void listJobsCollectionDoesNotExist() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection for this job does not exist
    doThrow(new MissingObjectException("Collection"))
        .when(collectionService)
        .validateCollection(collectionId.id());

    // Act / assert
    Exception actual =
        assertThrows(
            MissingObjectException.class,
            () -> jobService.getJobsForCollection(collectionId, Optional.of(allStatuses)));

    // Assert
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** Collection exists, associated with default workspace */
  @Test
  void listJobsDefaultCollection() {
    // Arrange
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // CollectionService.validateCollection() does not throw

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
}
