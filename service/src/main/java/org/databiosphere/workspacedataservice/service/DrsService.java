package org.databiosphere.workspacedataservice.service;

import java.net.URI;
import java.util.List;
import org.databiosphere.workspacedataservice.drshub.DrsHubApi;
import org.databiosphere.workspacedataservice.drshub.ResolveDrsRequest;
import org.databiosphere.workspacedataservice.drshub.ResourceMetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DrsService {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final DrsHubApi drsHubApi;

  public DrsService(DrsHubApi drsHubApi) {
    this.drsHubApi = drsHubApi;
  }

  /**
   * Check if the URI is a DRS URI
   *
   * @return true if the URI scheme is "drs", false otherwise
   */
  public boolean isDrsUri(URI uri) {
    return "drs".equalsIgnoreCase(uri.getScheme());
  }

  /**
   * Resolve a DRS URI to get the actual URL
   *
   * @param drsUri the DRS URI to resolve
   * @return the resolved URL
   */
  public URI resolveDrsUri(URI drsUri) {
    logger.info("Resolving DRS URI {}", drsUri);
    try {
      ResolveDrsRequest drsRequest = new ResolveDrsRequest(drsUri.toString(), List.of("accessUrl"));
      ResourceMetadataResponse resourceMetadataResponse = drsHubApi.resolveDrs(drsRequest);
      logger.info("Resolved DRS URI successfully");
      return resourceMetadataResponse.accessUrl().url();
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not resolve DRS URI: " + e.getMessage());
    }
  }
}
