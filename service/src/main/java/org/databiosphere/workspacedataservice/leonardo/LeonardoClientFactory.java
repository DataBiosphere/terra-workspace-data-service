package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;

public interface LeonardoClientFactory {
    AppsV2Api getAppsV2Api(String token);
}
