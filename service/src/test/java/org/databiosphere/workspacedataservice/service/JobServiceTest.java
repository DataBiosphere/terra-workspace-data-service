package org.databiosphere.workspacedataservice.service;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.databiosphere.workspacedataservice.annotations.WithTestObservationRegistry;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(
    profiles = {
      "data-plane", // need _a_ profile even if this test only contains tests for common behavior
    })
@DirtiesContext
@SpringBootTest(properties = {"twds.instance.workspace-id=f01dab1e-0000-1111-2222-000011112222"})
@WithTestObservationRegistry
class JobServiceTest extends JobServiceTestBase {
  @Autowired JobService jobService;
  @Autowired MeterRegistry meterRegistry;
  @Autowired TestObservationRegistry observationRegistry;
  @MockBean JobDao jobDao;

  @Test
  void processJobStatusUpdateSuccess() {
    // Arrange
    UUID jobId = stubJob(makeJob(StatusEnum.RUNNING));
    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, times(1)).succeeded(jobId);
  }

  @Test
  void processJobStatusUpdateError() {
    // Arrange
    UUID jobId = stubJob(makeJob(StatusEnum.RUNNING));
    JobStatusUpdate update =
        new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.ERROR, "Something went wrong");

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, times(1)).fail(jobId, "Something went wrong");
  }

  @Test
  void processJobStatusUpdateNoop() {
    // Arrange
    UUID jobId = stubJob(makeJob(StatusEnum.RUNNING));
    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.RUNNING);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    verify(jobDao, never()).running(jobId);
  }

  @Test
  void processJobStatusUpdateForTerminalJob() {
    // Arrange
    UUID jobId = stubJob(makeJob(StatusEnum.SUCCEEDED));
    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.SUCCEEDED, StatusEnum.RUNNING);

    // Act/Assert
    ValidationException e =
        assertThrows(ValidationException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    assertEquals("Unable to update terminal status for job %s".formatted(jobId), e.getMessage());
    verify(jobDao, never()).succeeded(jobId);
  }

  @Test
  void processJobStatusUpdateForNonExistentJob() {
    // Arrange
    UUID jobId = randomUUID();
    when(jobDao.getJob(jobId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    JobStatusUpdate update = new JobStatusUpdate(jobId, StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act/Assert
    assertThrows(MissingObjectException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    verify(jobDao, never()).succeeded(jobId);
  }

  @Test
  void processJobStatusUpdateMeasuresDurationSinceCreationOnSuccess() {
    // Arrange
    GenericJobServerModel job = makeJob(StatusEnum.RUNNING);
    OffsetDateTime fiveMinutesAgo = OffsetDateTime.now(ZoneId.of("Z")).minusMinutes(5);
    stubJob(
        job.created(fiveMinutesAgo)
            .input(
                new ImmutableMap.Builder<>()
                    .put("uri", "https://some-uri")
                    .put("importType", "PFB")
                    .build()));

    JobStatusUpdate update =
        new JobStatusUpdate(job.getJobId(), StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    assertThat(observationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsWithNameEqualTo("wds.job.update", 1)
        .hasObservationWithNameEqualTo("wds.job.update")
        .that()
        .hasHighCardinalityKeyValue("jobId", job.getJobId().toString())
        .hasLowCardinalityKeyValue("jobType", "PFB")
        .hasLowCardinalityKeyValue("oldStatus", "RUNNING")
        .hasLowCardinalityKeyValue("newStatus", "SUCCEEDED")
        .hasBeenStarted()
        .hasBeenStopped();

    Tags expectedTags =
        Tags.of(
            Tag.of("jobType", "PFB"),
            Tag.of("oldStatus", "RUNNING"),
            Tag.of("newStatus", "SUCCEEDED"));

    Timer timer = requireNonNull(meterRegistry.find("wds.job.elapsed").timer());
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.MINUTES)).isGreaterThanOrEqualTo(5);
    assertThat(timer.getId().getTags()).containsAll(expectedTags);
  }

  @Test
  void processJobStatusUpdateMeasuresDurationSinceCreationOnError() {
    // Arrange
    GenericJobServerModel job = makeJob(StatusEnum.CANCELLED);
    OffsetDateTime fiveMinutesAgo = OffsetDateTime.now(ZoneId.of("Z")).minusHours(9);
    stubJob(
        job.created(fiveMinutesAgo)
            .input(
                new ImmutableMap.Builder<>()
                    .put("uri", "https://some-uri")
                    .put("importType", "PFB")
                    .build()));

    // simulate late arriving success after cancellation
    JobStatusUpdate update =
        new JobStatusUpdate(job.getJobId(), StatusEnum.RUNNING, StatusEnum.SUCCEEDED);

    // Act / Assert
    ValidationException exception =
        assertThrows(ValidationException.class, () -> jobService.processStatusUpdate(update));

    // Assert
    String expectedError = "Unable to update terminal status for job %s".formatted(job.getJobId());
    assertThat(exception).hasMessage(expectedError);

    assertThat(observationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsEqualTo(1)
        .hasNumberOfObservationsWithNameEqualTo("wds.job.update", 1)
        .hasObservationWithNameEqualTo("wds.job.update")
        .that()
        .hasHighCardinalityKeyValue("jobId", job.getJobId().toString())
        .hasLowCardinalityKeyValue("jobType", "PFB")
        .hasLowCardinalityKeyValue("oldStatus", "CANCELLED")
        .hasLowCardinalityKeyValue("newStatus", "SUCCEEDED")
        .hasBeenStarted()
        .thenError()
        .hasMessage(expectedError);

    Tags expectedTags =
        Tags.of(
            Tag.of("jobType", "PFB"),
            Tag.of("oldStatus", "CANCELLED"),
            Tag.of("newStatus", "SUCCEEDED"));

    Timer timer = requireNonNull(meterRegistry.find("wds.job.elapsed").timer());
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.HOURS)).isGreaterThanOrEqualTo(9);
    assertThat(timer.getId().getTags()).containsAll(expectedTags);
  }

  @Test
  void processJobStatusUpdateMeasuresDurationOnNoChange() {
    // Arrange
    GenericJobServerModel job = makeJob(StatusEnum.RUNNING);
    OffsetDateTime fiveMinutesAgo = OffsetDateTime.now(ZoneId.of("Z")).minusMinutes(3);
    stubJob(
        job.created(fiveMinutesAgo)
            .input(
                new ImmutableMap.Builder<>()
                    .put("uri", "https://some-uri")
                    .put("importType", "PFB")
                    .build()));

    JobStatusUpdate update =
        new JobStatusUpdate(job.getJobId(), StatusEnum.RUNNING, StatusEnum.RUNNING);

    // Act
    jobService.processStatusUpdate(update);

    // Assert
    assertThat(observationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsWithNameEqualTo("wds.job.update", 1)
        .hasObservationWithNameEqualTo("wds.job.update")
        .that()
        .hasHighCardinalityKeyValue("jobId", job.getJobId().toString())
        .hasLowCardinalityKeyValue("jobType", "PFB")
        .hasLowCardinalityKeyValue("oldStatus", "RUNNING")
        .hasLowCardinalityKeyValue("newStatus", "RUNNING")
        .hasBeenStarted()
        .hasBeenStopped();

    Tags expectedTags =
        Tags.of(
            Tag.of("jobType", "PFB"),
            Tag.of("oldStatus", "RUNNING"),
            Tag.of("newStatus", "RUNNING"),
            Tag.of("noop", "true"));

    Timer timer = requireNonNull(meterRegistry.find("wds.job.elapsed").timer());
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.MINUTES)).isGreaterThanOrEqualTo(3);
    assertThat(timer.getId().getTags()).containsAll(expectedTags);
  }

  private UUID stubJob(GenericJobServerModel job) {
    when(jobDao.getJob(job.getJobId())).thenReturn(job);
    return job.getJobId();
  }

  private static GenericJobServerModel makeJob(StatusEnum initialStatus) {
    // Use UTC because that's how Postgres stores it
    OffsetDateTime now = OffsetDateTime.now(ZoneId.of("Z"));
    return new GenericJobServerModel(
        /* jobId= */ randomUUID(),
        JobTypeEnum.DATA_IMPORT,
        /* instanceId= */ randomUUID(),
        initialStatus,
        /* created= */ now,
        /* updated= */ now);
  }
}
