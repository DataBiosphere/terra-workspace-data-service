package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.RAWLSJSON;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.databiosphere.workspacedataservice.shared.model.job.JobType.DATA_IMPORT;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.jobexec.JobDataMapReader;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.pubsub.RawlsJsonPublisher;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.quartz.JobExecutionContext;
import org.springframework.stereotype.Component;

@ControlPlane
@Component
public class RawlsJsonQuartzJob extends QuartzJob {
  private final ImportDetailsRetriever importDetailsRetriever;
  private final GcsStorage storage;
  private final PubSub pubSub;

  public RawlsJsonQuartzJob(
      DataImportProperties dataImportProperties,
      ObservationRegistry observationRegistry,
      JobDao jobDao,
      ImportDetailsRetriever importDetailsRetriever,
      GcsStorage storage,
      PubSub pubSub) {
    super(jobDao, observationRegistry, dataImportProperties);
    this.importDetailsRetriever = importDetailsRetriever;
    this.storage = storage;
    this.pubSub = pubSub;
  }

  @Override
  protected void annotateObservation(Observation observation) {
    observation.lowCardinalityKeyValue("jobType", DATA_IMPORT.toString());
    observation.lowCardinalityKeyValue("importType", RAWLSJSON.toString());
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    JobDataMapReader jobData = JobDataMapReader.fromContext(context);
    ImportDetails details = importDetailsRetriever.fetch(jobId, jobData, PrefixStrategy.NONE);
    ImportJobInput jobInput = details.importJobInput();
    RawlsJsonImportOptions options = (RawlsJsonImportOptions) jobInput.getOptions();
    URI sourceUri = jobData.getURI(ARG_URL);
    Blob destination = moveBlob(sourceUri, rawlsJsonBlobName(jobId));
    publishToRawls(jobId, jobData, destination, options.isUpsert());
  }

  private Blob moveBlob(URI sourceUri, String desiredBlobName) {
    return storage.moveBlob(sourceUri, BlobId.of(storage.getBucketName(), desiredBlobName));
  }

  private ImportDetails getImportDetails(UUID jobId, JobDataMapReader jobData) {
    return importDetailsRetriever.fetch(jobId, jobData, PrefixStrategy.NONE);
  }

  private void publishToRawls(
      UUID jobId, JobDataMapReader jobData, Blob destination, boolean isUpsert) {
    new RawlsJsonPublisher(pubSub, getImportDetails(jobId, jobData), destination, isUpsert)
        .publish();
  }

  public static String rawlsJsonBlobName(UUID jobId) {
    return "%s-rawls-batch-write.json".formatted(jobId.toString());
  }
}
