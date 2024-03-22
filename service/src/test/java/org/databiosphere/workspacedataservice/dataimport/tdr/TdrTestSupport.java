package org.databiosphere.workspacedataservice.dataimport.tdr;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import java.net.URL;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupportFactory;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
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
  @Autowired private CollectionService collectionService;
  @Autowired private ActivityLogger activityLogger;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private ObservationRegistry observationRegistry;
  @Autowired DataImportProperties dataImportProperties;
  @Autowired private SnapshotSupportFactory snapshotSupportFactory;
  @Autowired private SamDao samDao;

  /** Returns a TdrManifestQuartzJob that is capable of pulling parquet files from the classpath. */
  TdrManifestQuartzJob buildTdrManifestQuartzJob(UUID workspaceId) {
    return new TdrManifestQuartzJob(
        jobDao,
        recordSourceFactory,
        recordSinkFactory,
        batchWriteService,
        collectionService,
        activityLogger,
        objectMapper,
        observationRegistry,
        dataImportProperties,
        snapshotSupportFactory,
        samDao) {
      @Override
      protected URL parseUrl(String path) {
        if (path.startsWith("classpath:")) {
          return requireNonNull(
              getClass().getClassLoader().getResource(path.substring("classpath:".length())));
        }
        return super.parseUrl(path);
      }
    };
  }
}
