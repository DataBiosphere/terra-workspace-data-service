package org.databiosphere.workspacedataservice.dataimport;

import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.springframework.stereotype.Component;

@Component
public class ImportSourceValidatorFactory {
  private final DataImportProperties properties;

  ImportSourceValidatorFactory(DataImportProperties properties) {
    this.properties = properties;
  }

  public ImportSourceValidator create(TypeEnum importType) {
    // TODO(AJ-1798): ImportSourceValidator supports all format / platform combinations
    if (importType == TypeEnum.RAWLSJSON) {
      return new ImportSourceValidator(
          /* allowedSchemes= */ Set.of("gs"),
          /* allowedHosts= */ Set.of(Pattern.compile(properties.getRawlsJsonDirectImportBucket())));
    }

    return new ImportSourceValidator(properties.getAllowedSchemes(), properties.getAllowedHosts());
  }
}
