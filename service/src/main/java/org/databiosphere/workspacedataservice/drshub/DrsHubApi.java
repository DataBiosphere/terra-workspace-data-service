package org.databiosphere.workspacedataservice.drshub;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.PostExchange;

/**
 * DrsHub API signatures. Use these to create http proxies for outbound requests via e.g.
 * RestClient.
 */
public interface DrsHubApi {

  /**
   * Resolve a DRS URI to a DRS object.
   *
   * @param resolveDrsRequest the DRS URI to resolve
   * @return the DRS object
   */
  @PostExchange("/api/v4/drs/resolve")
  ResourceMetadataResponse resolveDrs(@RequestBody ResolveDrsRequest resolveDrsRequest);
}
