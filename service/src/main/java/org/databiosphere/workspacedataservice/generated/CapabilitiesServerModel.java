package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.HashMap;
import java.util.Map;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * CapabilitiesServerModel
 */

@JsonTypeName("Capabilities")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class CapabilitiesServerModel extends HashMap<String, Boolean> {

  private Boolean capabilities;

  public CapabilitiesServerModel capabilities(Boolean capabilities) {
    this.capabilities = capabilities;
    return this;
  }

  /**
   * Get capabilities
   * @return capabilities
  */
  
  @Schema(name = "capabilities", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("capabilities")
  public Boolean getCapabilities() {
    return capabilities;
  }

  public void setCapabilities(Boolean capabilities) {
    this.capabilities = capabilities;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CapabilitiesServerModel capabilities = (CapabilitiesServerModel) o;
    return Objects.equals(this.capabilities, capabilities.capabilities) &&
        super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(capabilities, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CapabilitiesServerModel {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    capabilities: ").append(toIndentedString(capabilities)).append("\n");
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

