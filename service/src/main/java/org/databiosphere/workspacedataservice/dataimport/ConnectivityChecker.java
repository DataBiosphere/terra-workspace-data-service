package org.databiosphere.workspacedataservice.dataimport;

import java.io.IOException;
import java.net.URI;

interface ConnectivityChecker {
  boolean validateConnectivity(URI importUrl) throws IOException;
}
