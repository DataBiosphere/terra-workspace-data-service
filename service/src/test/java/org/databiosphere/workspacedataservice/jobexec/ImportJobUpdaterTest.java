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
public class ImportJobUpdaterTest extends TestBase {

  @Autowired PostgresJobDao jobDao;
  @Autowired MockInstantSource mockInstantSource;

  @Test
  public void testUpdateImportJobs() throws URISyntaxException {
    // Arrange
    JobType jobType = JobType.DATA_IMPORT;
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    ImportJobInput jobInput =
        new ImportJobInput(new URI("http://some/uri"), ImportRequestServerModel.TypeEnum.PFB);

    Job<JobInput, JobResult> testJob = Job.newJob(collectionId, jobType, jobInput);

    GenericJobServerModel job = jobDao.createJob(testJob);
    // TODO will import job updater only do running or also queued and created jobs?
    jobDao.running(job.getJobId());

    GenericJobServerModel createdJob = jobDao.getJob(job.getJobId());
    assertEquals(StatusEnum.RUNNING, createdJob.getStatus());
    mockInstantSource.add(Duration.ofHours(7));

    ImportJobUpdater updater = new ImportJobUpdater(jobDao);

    // Act
    updater.updateImportJobs();

    // Assert
    GenericJobServerModel updatedJob = jobDao.getJob(job.getJobId());
    assertEquals(StatusEnum.ERROR, updatedJob.getStatus());
  }
}
