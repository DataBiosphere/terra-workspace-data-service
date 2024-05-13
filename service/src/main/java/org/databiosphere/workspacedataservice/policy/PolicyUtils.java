package org.databiosphere.workspacedataservice.policy;

import static java.util.Collections.emptyList;

import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.Optional;
import org.springframework.lang.Nullable;

public final class PolicyUtils {
  public static final String TERRA_NAMESPACE = "terra";
  public static final String PROTECTED_DATA_POLICY_NAME = "protected-data";

  public static boolean isProtectedDataPolicy(WsmPolicyInput wsmPolicyInput) {
    return wsmPolicyInput.getNamespace().equals(TERRA_NAMESPACE)
        && wsmPolicyInput.getName().equals(PROTECTED_DATA_POLICY_NAME);
  }

  public static boolean containsProtectedDataPolicy(@Nullable List<WsmPolicyInput> policies) {
    return Optional.ofNullable(policies).orElse(emptyList()).stream()
        .anyMatch(PolicyUtils::isProtectedDataPolicy);
  }

  private PolicyUtils() {
    throw new IllegalStateException("Utility class");
  }
}
