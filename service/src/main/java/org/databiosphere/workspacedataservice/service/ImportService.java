package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.dataimport.ImportValidator;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbSchedulable;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonSchedulable;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestSchedulable;
import org.databiosphere.workspacedataservice.drshub.DrsHubApi;
import org.databiosphere.workspacedataservice.drshub.ResolveDrsRequest;
import org.databiosphere.workspacedataservice.drshub.ResourceMetadataResponse;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

@Service
public class ImportService {
  public static final String ARG_IMPORT_JOB_INPUT = "importJobInput";
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final CollectionService collectionService;
  private final SamDao samDao;
  private final JobDao jobDao;
  private final SchedulerDao schedulerDao;
  private final ImportValidator importValidator;
  private final DrsHubApi drsHubApi;

  public ImportService(
      CollectionService collectionService,
      SamDao samDao,
      JobDao jobDao,
      SchedulerDao schedulerDao,
      ImportValidator importValidator,
      DrsHubApi drsHubApi) {
    this.collectionService = collectionService;
    this.samDao = samDao;
    this.jobDao = jobDao;
    this.schedulerDao = schedulerDao;
    this.importValidator = importValidator;
    this.drsHubApi = drsHubApi;
  }

  public GenericJobServerModel createImport(
      UUID collectionId, ImportRequestServerModel importRequest) {

    // if the URI is a DRS URI, resolve it to get the actual URL
    if (isDrsUri(importRequest.getUrl())) {
      importRequest.setUrl(resolveDrsUri(importRequest.getUrl()));
    }

    // validate
    WorkspaceId workspaceId = collectionService.getWorkspaceId(CollectionId.of(collectionId));
    importValidator.validateImport(importRequest, workspaceId);

    // get a token to execute the job
    String petToken = samDao.getPetToken();

    logger.info("Data import of type {} requested", importRequest.getType());

    ImportJobInput importJobInput = ImportJobInput.from(importRequest);
    Job<JobInput, JobResult> job =
        Job.newJob(CollectionId.of(collectionId), JobType.DATA_IMPORT, importJobInput);

    // persist the full job to WDS's db
    GenericJobServerModel createdJob = jobDao.createJob(job);
    logger.debug(
        "Job {} created for data import of type {}",
        createdJob.getJobId(),
        importRequest.getType());

    try {
      // create the arguments for the schedulable job
      Map<String, Serializable> arguments = new HashMap<>();
      arguments.put(ARG_TOKEN, petToken);
      arguments.put(ARG_URL, importRequest.getUrl().toString());
      arguments.put(ARG_COLLECTION, collectionId.toString());
      arguments.put(ARG_IMPORT_JOB_INPUT, importJobInput);

      // if we can find an MDC id, add it to the job context
      safeGetMdcId(createdJob.getJobId())
          .ifPresent(requestId -> arguments.put(MDCServletRequestListener.MDC_KEY, requestId));

      // create the executable job to be scheduled
      Schedulable schedulable =
          createSchedulable(importRequest.getType(), createdJob.getJobId(), arguments);

      // schedule the job. after successfully scheduling, mark the job as queued
      schedulerDao.schedule(schedulable);
      logger.debug("Job {} scheduled", createdJob.getJobId());
    } catch (Exception e) {
      // we ran into a problem scheduling the job after we inserted the row in WDS's tracking table.
      // since this job won't run, mark it as failed.
      jobDao.fail(job.getJobId(), e);
      return createdJob;
    }

    // we successfully scheduled the job; mark it as queued.
    jobDao.queued(job.getJobId());

    // return the queued job
    return createdJob;
  }

  // attempt to get the requestId from MDC. We expect this to always succeed, but if it doesn't,
  // don't fail the import job. We only need the requestId for logging/correlation.
  private Optional<String> safeGetMdcId(UUID jobId) {
    try {
      return Optional.of(MDC.get(MDCServletRequestListener.MDC_KEY));
    } catch (Exception e) {
      logger.warn("Could not add MDC requestId to job map for job {}: {}", jobId, e.getMessage());
      return Optional.empty();
    }
  }

  @VisibleForTesting
  public static Schedulable createSchedulable(
      TypeEnum importType, UUID jobId, Map<String, Serializable> arguments) {
    return switch (importType) {
      case PFB -> new PfbSchedulable(jobId.toString(), "PFB import", arguments);
      case RAWLSJSON -> new RawlsJsonSchedulable(jobId.toString(), "RAWLSJSON import", arguments);
      case TDRMANIFEST -> new TdrManifestSchedulable(
          jobId.toString(), "TDR manifest import", arguments);
    };
  }

  /**
   * Check if the URI is a DRS URI
   *
   * @return true if the URI scheme is "drs", false otherwise
   */
  @VisibleForTesting
  public static boolean isDrsUri(URI uri) {
    return "drs".equalsIgnoreCase(uri.getScheme());
  }

  /**
   * Resolve a DRS URI to get the actual URL
   *
   * @param drsUri the DRS URI to resolve
   * @return the resolved URL
   */
  @VisibleForTesting
  public URI resolveDrsUri(URI drsUri) {
    logger.info("Resolving DRS URI {}", drsUri);
    try {
      ResolveDrsRequest drsRequest = new ResolveDrsRequest(drsUri.toString(), List.of("accessUrl"));
      ResourceMetadataResponse resourceMetadataResponse = drsHubApi.resolveDrs(drsRequest);
      URI resolvedUri = resourceMetadataResponse.accessUrl().url();
      logger.info("Resolved DRS URI to {}", resolvedUri);
      return resolvedUri;
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not resolve DRS URI: " + e.getMessage(), e);
    }
  }
}
