package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;

public class MockSamUsersApi extends UsersApi {
  public static final String MOCK_USER_SUBJECT_ID = "mock-user-subject-id";
  public static final String MOCK_USER_EMAIL = "mock@user.email";

  @Override
  public UserStatusInfo getUserStatusInfo() {
    return new UserStatusInfo().userSubjectId(MOCK_USER_SUBJECT_ID).userEmail(MOCK_USER_EMAIL);
  }
}
