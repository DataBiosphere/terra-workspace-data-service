package org.databiosphere.workspacedataservice.policy;

import bio.terra.workspace.model.WsmPolicyInput;

public final class PolicyUtils {
  public static final String TERRA_NAMESPACE = "terra";
  public static final String PROTECTED_DATA_POLICY_NAME = "protected-data";

  public static boolean isProtectedDataPolicy(WsmPolicyInput wsmPolicyInput) {
    return wsmPolicyInput.getNamespace().equals(TERRA_NAMESPACE)
        && wsmPolicyInput.getName().equals(PROTECTED_DATA_POLICY_NAME);
  }

  private PolicyUtils() {
    throw new IllegalStateException("Utility class");
  }
}
