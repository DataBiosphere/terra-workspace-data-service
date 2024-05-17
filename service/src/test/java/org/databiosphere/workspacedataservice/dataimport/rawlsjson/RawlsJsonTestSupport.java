package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob.OPTION_IS_UPSERT;
import static org.databiosphere.workspacedataservice.service.ImportService.ARG_IMPORT_JOB_INPUT;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.micrometer.observation.ObservationRegistry;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.JsonUtils;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy // only used by a few tests; don't instantiate when not needed
@Component
public class RawlsJsonTestSupport {
  @Autowired private DataImportProperties dataImportProperties;
  @Autowired private ObservationRegistry observations;
  @Autowired private JobDao jobDao;
  @Autowired private ImportDetailsRetriever importDetailsRetriever;
  @Autowired private GcsStorage storage;
  @Autowired private PubSub pubSub;

  RawlsJsonQuartzJob buildRawlsJsonQuartzJob() {
    return new RawlsJsonQuartzJob(
        dataImportProperties, observations, jobDao, importDetailsRetriever, storage, pubSub);
  }

  static JobExecutionContext stubJobContext(UUID jobId, URI resourceUri, UUID collectionId) {
    return stubJobContext(jobId, resourceUri, collectionId, Map.of());
  }

  static JobExecutionContext stubJobContext(
      UUID jobId, URI resourceUri, UUID collectionId, Map<String, Object> importOptions) {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);

    ImportJobInput importJobInput =
        new ImportJobInput(
            URI.create("gs://test-bucket/rawls-import.json"), TypeEnum.RAWLSJSON, importOptions);

    var schedulable =
        ImportService.createSchedulable(
            ImportRequestServerModel.TypeEnum.RAWLSJSON,
            jobId,
            new ImmutableMap.Builder<String, Serializable>()
                .put(ARG_TOKEN, "fake-bearer-token")
                .put(ARG_URL, resourceUri.toString())
                .put(ARG_COLLECTION, collectionId.toString())
                .put(ARG_IMPORT_JOB_INPUT, JsonUtils.stringify(importJobInput))
                .build());

    JobDetail jobDetail = schedulable.getJobDetail();
    when(mockContext.getMergedJobDataMap()).thenReturn(jobDetail.getJobDataMap());
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    return mockContext;
  }

  static JobExecutionContext stubJobContext(
      UUID jobId, URI resourceUri, UUID collectionId, boolean isUpsert) {

    return stubJobContext(jobId, resourceUri, collectionId, Map.of(OPTION_IS_UPSERT, isUpsert));
  }
}
