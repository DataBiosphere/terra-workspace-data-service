package org.databiosphere.workspacedataservice.dataimport.pfb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class PfbTestSupport {
  @Autowired private JobDao jobDao;
  @Autowired private WorkspaceManagerDao wsmDao;
  @Autowired private RestClientRetry restClientRetry;
  @Autowired private BatchWriteService batchWriteService;
  @Autowired private ActivityLogger activityLogger;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private ObservationRegistry observationRegistry;

  PfbQuartzJob buildPfbQuartzJob(UUID workspaceId) {
    return new PfbQuartzJob(
        jobDao,
        wsmDao,
        restClientRetry,
        batchWriteService,
        activityLogger,
        observationRegistry,
        workspaceId);
  }

  PfbQuartzJob buildPfbQuartzJob() {
    return buildPfbQuartzJob(UUID.randomUUID());
  }
}
