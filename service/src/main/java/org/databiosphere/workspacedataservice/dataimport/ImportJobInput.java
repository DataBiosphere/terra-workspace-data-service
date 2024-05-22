package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbImportOptions;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbJobInput;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonJobInput;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

/** User-supplied input arguments for a data import job */
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
public class ImportJobInput implements JobInput, Serializable {
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
    return switch (importRequest.getType()) {
      case PFB ->
          new PfbJobInput(
              importRequest.getUrl(),
              importRequest.getType(),
              PfbImportOptions.from(importRequest.getOptions()));
      case RAWLSJSON ->
          new RawlsJsonJobInput(
              importRequest.getUrl(),
              importRequest.getType(),
              RawlsJsonImportOptions.from(importRequest.getOptions()));
      case TDRMANIFEST ->
          new TdrManifestJobInput(
              importRequest.getUrl(),
              importRequest.getType(),
              TdrManifestImportOptions.from(importRequest.getOptions()));
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

  //  public static class ImportJobInputDeserializer extends JsonDeserializer<ImportJobInput> {
  //    @Override
  //    @Nullable
  //    public ImportJobInput deserialize(JsonParser parser, DeserializationContext context)
  //        throws IOException {
  //      ObjectMapper mapper = (ObjectMapper) parser.getCodec();
  //      JsonNode node = parser.readValueAsTree();
  //
  //      URI uri = URI.create(node.get("uri").asText());
  //      TypeEnum type = TypeEnum.fromValue(node.get("importType").asText());
  //
  //      JsonNode optionsNode = node.get("options");
  //      ImportOptions options =
  //          switch (type) {
  //            case PFB -> mapper.convertValue(optionsNode, PfbImportOptions.class);
  //            case RAWLSJSON -> mapper.convertValue(optionsNode, RawlsJsonImportOptions.class);
  //            case TDRMANIFEST -> mapper.convertValue(optionsNode,
  // TdrManifestImportOptions.class);
  //          };
  //
  //      return new ImportJobInput(uri, type, options);
  //    }
  //  }
}
