package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbJobInput;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonJobInput;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

/**
 * Abstract base class representing user-supplied input arguments for a data import job.
 *
 * <p>Each type of import (pfb, tdrmanifest, etc.) has a concrete subclass.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "importType",
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    visible = true)
@JsonSubTypes({
  @Type(value = PfbJobInput.class, name = "PFB"),
  @Type(value = RawlsJsonJobInput.class, name = "RAWLSJSON"),
  @Type(value = TdrManifestJobInput.class, name = "TDRMANIFEST"),
})
public abstract class ImportJobInput implements JobInput, Serializable {
  private final URI uri;
  private final TypeEnum importType;
  private final ImportOptions options;

  // protected constructor; should only be used by subclasses
  protected ImportJobInput(URI uri, TypeEnum importType, ImportOptions options) {
    this.uri = uri;
    this.importType = importType;
    this.options = options;
  }

  /**
   * Convenience method to create a concrete subclass based on user input
   *
   * @param importRequest the user-supplied input
   * @return the subclass for this input
   */
  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    return switch (importRequest.getType()) {
      case PFB -> PfbJobInput.from(importRequest);
      case RAWLSJSON -> RawlsJsonJobInput.from(importRequest);
      case TDRMANIFEST -> TdrManifestJobInput.from(importRequest);
    };
  }

  public URI getUri() {
    return uri;
  }

  public TypeEnum getImportType() {
    return importType;
  }

  public ImportOptions getOptions() {
    return options;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (ImportJobInput) obj;
    return Objects.equals(this.uri, that.uri)
        && Objects.equals(this.importType, that.importType)
        && Objects.equals(this.options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, importType, options);
  }

  @Override
  public String toString() {
    return this.getClass().getName()
        + "["
        + "uri="
        + uri
        + ", "
        + "importType="
        + importType
        + ", "
        + "options="
        + options
        + ']';
  }
}
