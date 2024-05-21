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
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.lang.Nullable;

/** User-supplied input arguments for a data import job */
@JsonDeserialize(using = ImportJobInput.ImportJobInputDeserializer.class)
public record ImportJobInput(URI uri, TypeEnum importType, ImportOptions options)
    implements JobInput, Serializable {

  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    ImportOptions options =
        switch (importRequest.getType()) {
          case PFB -> PfbImportOptions.from(importRequest.getOptions());
          case RAWLSJSON -> RawlsJsonImportOptions.from(importRequest.getOptions());
          case TDRMANIFEST -> TdrManifestImportOptions.from(importRequest.getOptions());
        };

    return new ImportJobInput(importRequest.getUrl(), importRequest.getType(), options);
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
