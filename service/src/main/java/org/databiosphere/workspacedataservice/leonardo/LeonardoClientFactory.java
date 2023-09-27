package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsApi;

public interface LeonardoClientFactory {
  AppsApi getAppsV2Api(String token);
}
