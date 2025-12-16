package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;

import io.micrometer.observation.ObservationRegistry;
import java.io.IOException;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.ProtectedDataSupport;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.MultiCloudSnapshotSupportFactory;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.metrics.ImportMetrics;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DrsService;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

@Lazy // only used by a few tests; don't instantiate when not needed
@Component
class PfbTestSupport {
  @Autowired private JobDao jobDao;
  @Autowired private RecordSourceFactory recordSourceFactory;
  @Autowired private RecordSinkFactory recordSinkFactory;
  @Autowired private BatchWriteService batchWriteService;
  @Autowired private ActivityLogger activityLogger;
  @Autowired private ObservationRegistry observationRegistry;
  @Autowired private ImportMetrics importMetrics;
  @Autowired private ImportService importService;
  @Autowired private MultiCloudSnapshotSupportFactory snapshotSupportFactory;
  @Autowired private DataImportProperties dataImportProperties;
  @Autowired private ImportDetailsRetriever importDetailsRetriever;
  @Autowired private DrsService drsService;
  @Autowired private ProtectedDataSupport protectedDataSupport;

  UUID executePfbImportQuartzJob(UUID collectionId, Resource pfbResource)
      throws IOException, JobExecutionException {
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.PFB, pfbResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(collectionId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, pfbResource, collectionId);

    buildPfbQuartzJob().execute(mockContext);

    return jobId;
  }

  PfbQuartzJob buildPfbQuartzJob() {
    return new PfbQuartzJob(
        jobDao,
        recordSourceFactory,
        recordSinkFactory,
        batchWriteService,
        activityLogger,
        observationRegistry,
        importMetrics,
        snapshotSupportFactory,
        dataImportProperties,
        importDetailsRetriever,
        drsService,
        protectedDataSupport);
  }
}
