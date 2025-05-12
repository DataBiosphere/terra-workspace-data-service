package org.databiosphere.workspacedataservice.dataimport.tdr;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import java.net.URL;
import java.time.InstantSource;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.MultiCloudSnapshotSupportFactory;
import org.databiosphere.workspacedataservice.metrics.ImportMetrics;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy // only used by a few tests; don't instantiate when not needed
@Component
class TdrTestSupport {
  @Autowired private JobDao jobDao;
  @Autowired private SamDao samDao;
  @Autowired private RecordSourceFactory recordSourceFactory;
  @Autowired private RecordSinkFactory recordSinkFactory;
  @Autowired private BatchWriteService batchWriteService;
  @Autowired private ImportDetailsRetriever importDetailsRetriever;
  @Autowired private ActivityLogger activityLogger;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private ObservationRegistry observationRegistry;
  @Autowired private ImportMetrics importMetrics;
  @Autowired private DataImportProperties dataImportProperties;
  @Autowired private MultiCloudSnapshotSupportFactory snapshotSupportFactory;
  @Autowired private InstantSource instantSource;

  /** Returns a TdrManifestQuartzJob that is capable of pulling parquet files from the classpath. */
  TdrManifestQuartzJob buildTdrManifestQuartzJob() {
    return new TdrManifestQuartzJob(
        jobDao,
        recordSourceFactory,
        recordSinkFactory,
        batchWriteService,
        activityLogger,
        objectMapper,
        observationRegistry,
        importMetrics,
        dataImportProperties,
        snapshotSupportFactory,
        samDao,
        importDetailsRetriever,
        instantSource) {
      @Override
      protected URL parseUrl(String path) {
        if (path.startsWith("classpath:")) {
          var url = getClass().getClassLoader().getResource(path.substring("classpath:".length()));

          if (url == null) {
            throw new RuntimeException("Unable to load classpath resource %s".formatted(path));
          }

          return url;
        }
        return super.parseUrl(path);
      }
    };
  }
}
