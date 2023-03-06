package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;

/**
 * interface for SamClientFactory, allowing verious implementations:
 * - HttpSamClientFactory, which generates an ApiClient and ResourcesApi from the Sam client library;
 * - MockSamClientFactory, which generates a mock ResourcesApi for unit testing or local development
 */
public interface SamClientFactory {

    public ResourcesApi getResourcesApi();
}
