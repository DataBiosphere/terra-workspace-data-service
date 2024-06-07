package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.databiosphere.workspacedataservice.service.ImportService.ARG_IMPORT_JOB_INPUT;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.springframework.core.io.Resource;

public class TdrManifestTestUtils {
  public static final String BEARER_TOKEN = "expectedToken";

  public static JobExecutionContext stubJobContext(UUID jobId, Resource resource, UUID collectionId)
      throws IOException {
    return stubJobContext(jobId, resource, collectionId, /* syncPermissions= */ false);
  }

  public static JobExecutionContext stubJobContext(UUID jobId, URI uri, UUID collectionId) {
    return stubJobContext(jobId, uri, collectionId, /* syncPermissions= */ false);
  }

  public static JobExecutionContext stubJobContext(
      UUID jobId, Resource resource, UUID collectionId, boolean syncPermissions)
      throws IOException {
    return stubJobContext(jobId, resource.getURI(), collectionId, syncPermissions);
  }

  public static JobExecutionContext stubJobContext(
      UUID jobId, URI resourceUri, UUID collectionId, boolean syncPermissions) {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);

    ImportJobInput importJobInput =
        new TdrManifestJobInput(
            URI.create("https://data.terra.bio/manifest.json"),
            new TdrManifestImportOptions(syncPermissions));

    var schedulable =
        ImportService.createSchedulable(
            TypeEnum.TDRMANIFEST,
            jobId,
            new ImmutableMap.Builder<String, Serializable>()
                .put(ARG_TOKEN, BEARER_TOKEN)
                .put(ARG_URL, resourceUri.toString())
                .put(ARG_COLLECTION, collectionId.toString())
                .put(ARG_IMPORT_JOB_INPUT, importJobInput)
                .build());

    JobDetail jobDetail = schedulable.getJobDetail();
    when(mockContext.getMergedJobDataMap()).thenReturn(jobDetail.getJobDataMap());
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    return mockContext;
  }
}
