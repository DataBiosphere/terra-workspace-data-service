package org.databiosphere.workspacedataservice.policy;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.lang.Nullable;

class PolicyUtilsTest {
  @ParameterizedTest(name = "isProtectedDataPolicy {1}")
  @MethodSource("isProtectedDataTestCases")
  void isProtectedDataPolicy(WsmPolicyInput policy, boolean expected) {
    // Act
    boolean actual = PolicyUtils.isProtectedDataPolicy(policy);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> isProtectedDataTestCases() {
    WsmPolicyInput protectedDataPolicy = new WsmPolicyInput();
    protectedDataPolicy.setNamespace(PolicyUtils.TERRA_NAMESPACE);
    protectedDataPolicy.setName(PolicyUtils.PROTECTED_DATA_POLICY_NAME);

    WsmPolicyInput otherPolicy = new WsmPolicyInput();
    otherPolicy.setNamespace(PolicyUtils.TERRA_NAMESPACE);
    otherPolicy.setName("other-policy");

    return Stream.of(Arguments.of(protectedDataPolicy, true), Arguments.of(otherPolicy, false));
  }

  @ParameterizedTest(name = "containsProtectedDataTestCases {1}")
  @MethodSource("containsProtectedDataTestCases")
  void containsProtectedDataPolicy(@Nullable List<WsmPolicyInput> policies, boolean expected) {
    // Act
    boolean actual = PolicyUtils.containsProtectedDataPolicy(policies);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> containsProtectedDataTestCases() {
    WsmPolicyInput protectedDataPolicy = new WsmPolicyInput();
    protectedDataPolicy.setNamespace(PolicyUtils.TERRA_NAMESPACE);
    protectedDataPolicy.setName(PolicyUtils.PROTECTED_DATA_POLICY_NAME);

    WsmPolicyInput otherPolicy = new WsmPolicyInput();
    otherPolicy.setNamespace(PolicyUtils.TERRA_NAMESPACE);
    otherPolicy.setName("other-policy");

    return Stream.of(
        Arguments.of(List.of(protectedDataPolicy), true),
        Arguments.of(List.of(otherPolicy), false),
        Arguments.of(emptyList(), false),
        Arguments.of(null, false));
  }
}
