package org.databiosphere.workspacedataservice.service;

import static java.time.OffsetDateTime.now;
import static java.util.Objects.requireNonNullElse;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

@Service
public class JobService {
  private static final Set<StatusEnum> TERMINAL_JOB_STATUSES =
      Set.of(StatusEnum.SUCCEEDED, StatusEnum.ERROR, StatusEnum.CANCELLED);
  private static final String UNKNOWN = "UNKNOWN";
  private static final Logger logger = LoggerFactory.getLogger(JobService.class);

  private final JobDao jobDao;
  private final CollectionService collectionService;
  private final MeterRegistry meterRegistry;
  private final ObservationRegistry observationRegistry;

  public JobService(
      JobDao jobDao,
      CollectionService collectionService,
      MeterRegistry meterRegistry,
      ObservationRegistry observationRegistry) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
    this.meterRegistry = meterRegistry;
    this.observationRegistry = observationRegistry;
  }

  public GenericJobServerModel getJob(UUID jobId) {
    try {
      GenericJobServerModel result = jobDao.getJob(jobId);
      if (!collectionService.canReadCollection(CollectionId.of(result.getInstanceId()))) {
        throw new AuthenticationMaskableException("Job");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, Optional<List<String>> statuses) {
    // verify collection exists
    collectionService.validateCollection(collectionId.id());
    // check permissions
    if (!collectionService.canReadCollection(collectionId)) {
      throw new AuthenticationMaskableException("Collection");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }

  /**
   * Process a job status update from Rawls received via PubSub. This method is only used in the
   * control plane.
   */
  public void processStatusUpdate(JobStatusUpdate update) {
    UUID jobId = update.jobId();
    StatusEnum newStatus = update.newStatus();
    Observation observation =
        Observation.start("wds.job.update", observationRegistry)
            .highCardinalityKeyValue("jobId", jobId.toString())
            .lowCardinalityKeyValue("newStatus", newStatus.toString());

    GenericJobServerModel job = null;
    try {
      // JobService's getJob method checks permissions via Sam. Since this method is not run
      // as part of a user request, there is no token available to make requests to Sam with.
      // Thus, we must use jobDao's getJob here.
      job = jobDao.getJob(jobId);
      StatusEnum currentStatus = job.getStatus();

      // Ignore messages that don't change the job's status.
      // Rawls and import service have more granular statuses than CWDS, so the initial update
      // will be from "ReadyToUpsert" to "Upserting", which both translate to "RUNNING" in CWDS.
      if (currentStatus.equals(newStatus)) {
        observation.lowCardinalityKeyValue("noop", "true");
        return;
      }

      // Once a job has reached a terminal state, do not update its status again.
      if (TERMINAL_JOB_STATUSES.contains(currentStatus)) {
        throw new ValidationException(
            "Unable to update terminal status for job %s".formatted(jobId));
      }

      switch (newStatus) {
        case SUCCEEDED -> jobDao.succeeded(jobId);
        case ERROR -> jobDao.fail(
            jobId, requireNonNullElse(update.errorMessage(), "Unknown error"));
        default -> throw new ValidationException(
            "Unexpected status from Rawls for job %s: %s".formatted(jobId, newStatus));
      }
    } catch (Exception e) {
      // catchall for any exceptions that occur during the status update (including those explicitly
      // thrown in the try block); in all such cases, mark the observation as having an error
      observation.error(e);
      if (e instanceof EmptyResultDataAccessException) {
        throw new MissingObjectException("Job");
      }
      throw e;
    } finally {
      finalizeJobObservation(job, observation);
    }
  }

  private void finalizeJobObservation(
      @Nullable GenericJobServerModel job, Observation observation) {
    observation
        .lowCardinalityKeyValue("jobType", job != null ? getJobType(job) : UNKNOWN)
        .lowCardinalityKeyValue("oldStatus", job != null ? job.getStatus().toString() : UNKNOWN);

    if (job != null) {
      meterRegistry
          .timer("wds.job.elapsed", getTags(observation))
          .record(Duration.between(job.getCreated(), now()));
    }
    observation.stop();
  }

  private static String getJobType(GenericJobServerModel job) {
    try {
      Map<String, Object> jobInput = (Map<String, Object>) job.getInput();
      if (jobInput != null) {
        return requireNonNullElse(jobInput.get("importType"), UNKNOWN).toString();
      }
    } catch (Exception e) {
      logger.atWarn().log("Failed to get job type for job {}", job.getJobId());
    }
    return UNKNOWN;
  }

  private static Tags getTags(Observation observation) {
    return Tags.of(
        observation.getContext().getLowCardinalityKeyValues().stream()
            .map(keyValue -> Tag.of(keyValue.getKey(), keyValue.getValue()))
            .toList());
  }
}
