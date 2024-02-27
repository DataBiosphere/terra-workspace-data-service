package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;

public class MockStatusApi extends StatusApi {
  @Override
  public SystemStatus getSystemStatus() {
    return new SystemStatus().ok(true);
  }
}
