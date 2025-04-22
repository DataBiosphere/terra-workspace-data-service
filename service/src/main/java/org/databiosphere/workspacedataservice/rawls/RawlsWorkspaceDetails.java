package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.WsmPolicyInput;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.List;

public record RawlsWorkspaceDetails(
    @JsonProperty RawlsWorkspace workspace, @JsonProperty List<WsmPolicyInput> policies) {

  /**
   * RawlsWorkspaceDetails only includes the subset of workspace fields needed by CWDS. This list
   * defines the fields parameter for the Rawls getWorkspaceDetails request such that the request
   * will return only the fields included in RawlsWorkspaceDetails.
   */
  public static final List<String> SUPPORTED_FIELDS =
      List.of(
          "policies",
          "workspace.bucketName",
          "workspace.workspaceType",
          "workspace.namespace",
          "workspace.name");

  public record RawlsWorkspace(
      @JsonProperty String bucketName,
      @JsonProperty WorkspaceType workspaceType,
      @JsonProperty String namespace,
      @JsonProperty String name) {

    public enum WorkspaceType {
      MC("mc"),

      RAWLS("rawls");

      private final String value;

      WorkspaceType(String value) {
        this.value = value;
      }

      @JsonValue
      public String getValue() {
        return value;
      }

      @Override
      public String toString() {
        return String.valueOf(value);
      }

      @JsonCreator
      public static WorkspaceType fromValue(String value) {
        for (WorkspaceType candidate : WorkspaceType.values()) {
          if (candidate.value.equals(value)) {
            return candidate;
          }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
      }
    }
  }
}
