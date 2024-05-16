package org.databiosphere.workspacedataservice.jobexec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.MockInstantSource;
import org.databiosphere.workspacedataservice.common.MockInstantSourceConfig;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.PostgresJobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(MockInstantSourceConfig.class)
class ImportJobUpdaterTest extends TestBase {

  @Autowired PostgresJobDao jobDao;
  @Autowired MockInstantSource mockInstantSource;

  @Test
  void testUpdateImportJobs() throws URISyntaxException {
    // Arrange
    JobType jobType = JobType.DATA_IMPORT;
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    ImportJobInput jobInput =
        new ImportJobInput(new URI("http://some/uri"), ImportRequestServerModel.TypeEnum.PFB);

    // A running job
    Job<JobInput, JobResult> testJob = Job.newJob(collectionId, jobType, jobInput);
    GenericJobServerModel job = jobDao.createJob(testJob);
    jobDao.running(job.getJobId());

    // A queued job
    Job<JobInput, JobResult> testJob2 = Job.newJob(collectionId, jobType, jobInput);
    GenericJobServerModel job2 = jobDao.createJob(testJob2);
    jobDao.queued(job2.getJobId());

    // An errored job
    Job<JobInput, JobResult> testJob3 = Job.newJob(collectionId, jobType, jobInput);
    GenericJobServerModel job3 = jobDao.createJob(testJob3);
    jobDao.fail(job3.getJobId(), "Failing job for test");

    // Act
    // Let time pass, then run job updater
    mockInstantSource.add(Duration.ofHours(7));
    ImportJobUpdater updater = new ImportJobUpdater(jobDao);

    updater.updateImportJobs();

    // Assert
    GenericJobServerModel updatedJob1 = jobDao.getJob(job.getJobId());
    assertEquals(StatusEnum.ERROR, updatedJob1.getStatus());
    GenericJobServerModel updatedJob2 = jobDao.getJob(job2.getJobId());
    assertEquals(StatusEnum.ERROR, updatedJob2.getStatus());
    // The already-errored job should not have been updated
    GenericJobServerModel updatedJob3 = jobDao.getJob(job3.getJobId());
    assertEquals("Failing job for test", updatedJob3.getErrorMessage());
  }
}
