package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;
import org.springframework.lang.Nullable;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * when present, will clone all collections from sourceWorkspaceId into this workspace
 */

@Schema(name = "WorkspaceInitClone", description = "when present, will clone all collections from sourceWorkspaceId into this workspace")
@JsonTypeName("WorkspaceInitClone")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.11.0")
public class WorkspaceInitCloneServerModel {

  private UUID sourceWorkspaceId;

  public WorkspaceInitCloneServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public WorkspaceInitCloneServerModel(UUID sourceWorkspaceId) {
    this.sourceWorkspaceId = sourceWorkspaceId;
  }

  public WorkspaceInitCloneServerModel sourceWorkspaceId(UUID sourceWorkspaceId) {
    this.sourceWorkspaceId = sourceWorkspaceId;
    return this;
  }

  /**
   * id of the workspace being cloned
   * @return sourceWorkspaceId
   */
  @NotNull @Valid 
  @Schema(name = "sourceWorkspaceId", description = "id of the workspace being cloned", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("sourceWorkspaceId")
  public UUID getSourceWorkspaceId() {
    return sourceWorkspaceId;
  }

  public void setSourceWorkspaceId(UUID sourceWorkspaceId) {
    this.sourceWorkspaceId = sourceWorkspaceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkspaceInitCloneServerModel workspaceInitClone = (WorkspaceInitCloneServerModel) o;
    return Objects.equals(this.sourceWorkspaceId, workspaceInitClone.sourceWorkspaceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceWorkspaceId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkspaceInitCloneServerModel {\n");
    sb.append("    sourceWorkspaceId: ").append(toIndentedString(sourceWorkspaceId)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

