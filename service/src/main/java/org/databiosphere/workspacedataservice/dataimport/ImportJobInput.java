package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.lang.Nullable;

/** User-supplied input arguments for a data import job */
@JsonDeserialize(using = ImportJobInput.ImportJobInputDeserializer.class)
public final class ImportJobInput implements JobInput, Serializable {
  // TODO: decide what to do about serialVersionUID
  // @Serial private static final long serialVersionUID = 0L;
  private final URI uri;
  private final TypeEnum importType;
  private final ImportOptions options;

  public ImportJobInput(URI uri, TypeEnum importType, ImportOptions options) {
    this.uri = uri;
    this.importType = importType;
    this.options = options;
  }

  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    ImportOptions options =
        switch (importRequest.getType()) {
          case PFB -> PfbImportOptions.from(importRequest.getOptions());
          case RAWLSJSON -> RawlsJsonImportOptions.from(importRequest.getOptions());
          case TDRMANIFEST -> TdrManifestImportOptions.from(importRequest.getOptions());
        };

    return new ImportJobInput(importRequest.getUrl(), importRequest.getType(), options);
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
    return "ImportJobInput["
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

  public static class ImportJobInputDeserializer extends JsonDeserializer<ImportJobInput> {
    @Override
    @Nullable
    public ImportJobInput deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      ObjectMapper mapper = (ObjectMapper) parser.getCodec();
      JsonNode node = parser.readValueAsTree();

      URI uri = URI.create(node.get("uri").asText());
      TypeEnum type = TypeEnum.fromValue(node.get("importType").asText());

      JsonNode optionsNode = node.get("options");
      ImportOptions options =
          switch (type) {
            case PFB -> mapper.convertValue(optionsNode, PfbImportOptions.class);
            case RAWLSJSON -> mapper.convertValue(optionsNode, RawlsJsonImportOptions.class);
            case TDRMANIFEST -> mapper.convertValue(optionsNode, TdrManifestImportOptions.class);
          };

      return new ImportJobInput(uri, type, options);
    }
  }
}
