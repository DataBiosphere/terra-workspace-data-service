package org.databiosphere.workspacedataservice.sam;

import java.util.List;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;

/** Mock for the Sam Client GoogleApi for use in unit tests. Never throws any Exceptions. */
public class MockSamGoogleApi extends GoogleApi {

  @Override
  public String getArbitraryPetServiceAccountToken(List<String> scopes) {
    return "arbitraryToken";
  }
}
