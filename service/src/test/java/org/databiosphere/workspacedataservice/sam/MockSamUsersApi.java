package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;

public class MockSamUsersApi extends UsersApi {
  @Override
  public UserStatusInfo getUserStatusInfo() {
    return new UserStatusInfo().userSubjectId("mock-user-subject-id");
  }
}
