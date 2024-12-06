package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;

public class NoopConnectivityChecker implements ConnectivityChecker {
  /**
   * @param importUrl
   * @return
   */
  @Override
  public boolean validateConnectivity(URI importUrl) {
    return true;
  }
}
