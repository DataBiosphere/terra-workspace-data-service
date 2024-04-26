package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.RAWLSJSON;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.databiosphere.workspacedataservice.shared.model.job.JobType.DATA_IMPORT;

import com.google.cloud.storage.Blob;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
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
    URI sourceUri = jobData.getURI(ARG_URL);
    Boolean isUpsert = jobData.getBoolean("isUpsert");

    ImportDetails details = importDetailsRetriever.fetch(jobId, jobData, PrefixStrategy.NONE);

    Blob destination = storage.moveBlob(sourceUri, storage.createBlob(rawlsJsonBlobName(jobId)));

    new RawlsJsonPublisher(pubSub, details, destination, isUpsert).publish();
  }

  public static String rawlsJsonBlobName(UUID jobId) {
    return "%s-rawls-batch-write.json".formatted(jobId.toString());
  }
}
