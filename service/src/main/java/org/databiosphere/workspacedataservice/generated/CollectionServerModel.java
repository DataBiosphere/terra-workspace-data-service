package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * CollectionServerModel
 */

@JsonTypeName("Collection")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class CollectionServerModel {

  private UUID id;

  private UUID workspaceId;

  private String name;

  private String description;

  /**
   * Default constructor
   * @deprecated Use {@link CollectionServerModel#CollectionServerModel(UUID, UUID, String, String)}
   */
  @Deprecated
  public CollectionServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public CollectionServerModel(UUID id, UUID workspaceId, String name, String description) {
    this.id = id;
    this.workspaceId = workspaceId;
    this.name = name;
    this.description = description;
  }

  public CollectionServerModel id(UUID id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
  */
  @NotNull @Valid 
  @Schema(name = "id", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("id")
  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public CollectionServerModel workspaceId(UUID workspaceId) {
    this.workspaceId = workspaceId;
    return this;
  }

  /**
   * Get workspaceId
   * @return workspaceId
  */
  @NotNull @Valid 
  @Schema(name = "workspaceId", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("workspaceId")
  public UUID getWorkspaceId() {
    return workspaceId;
  }

  public void setWorkspaceId(UUID workspaceId) {
    this.workspaceId = workspaceId;
  }

  public CollectionServerModel name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
  */
  @NotNull 
  @Schema(name = "name", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CollectionServerModel description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * @return description
  */
  @NotNull 
  @Schema(name = "description", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CollectionServerModel collection = (CollectionServerModel) o;
    return Objects.equals(this.id, collection.id) &&
        Objects.equals(this.workspaceId, collection.workspaceId) &&
        Objects.equals(this.name, collection.name) &&
        Objects.equals(this.description, collection.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, workspaceId, name, description);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CollectionServerModel {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    workspaceId: ").append(toIndentedString(workspaceId)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
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

