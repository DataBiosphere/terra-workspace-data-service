package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.net.URI;
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
 * ImportRequestServerModel
 */

@JsonTypeName("ImportRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class ImportRequestServerModel {

  /**
   * format of file to import
   */
  public enum TypeEnum {
    PFB("PFB"),
    
    RAWLSJSON("RAWLSJSON"),
    
    TDRMANIFEST("TDRMANIFEST");

    private String value;

    TypeEnum(String value) {
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
    public static TypeEnum fromValue(String value) {
      for (TypeEnum b : TypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private TypeEnum type;

  private URI url;

  @Valid
  private Map<String, Object> options = new HashMap<>();

  public ImportRequestServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ImportRequestServerModel(TypeEnum type, URI url) {
    this.type = type;
    this.url = url;
  }

  public ImportRequestServerModel type(TypeEnum type) {
    this.type = type;
    return this;
  }

  /**
   * format of file to import
   * @return type
   */
  @NotNull 
  @Schema(name = "type", description = "format of file to import", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  public ImportRequestServerModel url(URI url) {
    this.url = url;
    return this;
  }

  /**
   * url from which to import
   * @return url
   */
  @NotNull @Valid 
  @Schema(name = "url", description = "url from which to import", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("url")
  public URI getUrl() {
    return url;
  }

  public void setUrl(URI url) {
    this.url = url;
  }

  public ImportRequestServerModel options(Map<String, Object> options) {
    this.options = options;
    return this;
  }

  public ImportRequestServerModel putOptionsItem(String key, Object optionsItem) {
    if (this.options == null) {
      this.options = new HashMap<>();
    }
    this.options.put(key, optionsItem);
    return this;
  }

  /**
   * key-value pairs to configure this import. Options vary based on the import file type.
   * @return options
   */
  
  @Schema(name = "options", description = "key-value pairs to configure this import. Options vary based on the import file type.", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("options")
  public Map<String, Object> getOptions() {
    return options;
  }

  public void setOptions(Map<String, Object> options) {
    this.options = options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImportRequestServerModel importRequest = (ImportRequestServerModel) o;
    return Objects.equals(this.type, importRequest.type) &&
        Objects.equals(this.url, importRequest.url) &&
        Objects.equals(this.options, importRequest.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, url, options);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ImportRequestServerModel {\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    url: ").append(toIndentedString(url)).append("\n");
    sb.append("    options: ").append(toIndentedString(options)).append("\n");
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

