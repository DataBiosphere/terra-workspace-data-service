package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitCloneServerModel;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * WorkspaceInitServerModel
 */

@JsonTypeName("WorkspaceInit")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.7.0")
public class WorkspaceInitServerModel {

  private WorkspaceInitCloneServerModel clone;

  public WorkspaceInitServerModel clone(WorkspaceInitCloneServerModel clone) {
    this.clone = clone;
    return this;
  }

  /**
   * Get clone
   * @return clone
   */
  @Valid 
  @Schema(name = "clone", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("clone")
  public WorkspaceInitCloneServerModel getClone() {
    return clone;
  }

  public void setClone(WorkspaceInitCloneServerModel clone) {
    this.clone = clone;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkspaceInitServerModel workspaceInit = (WorkspaceInitServerModel) o;
    return Objects.equals(this.clone, workspaceInit.clone);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clone);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkspaceInitServerModel {\n");
    sb.append("    clone: ").append(toIndentedString(clone)).append("\n");
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

