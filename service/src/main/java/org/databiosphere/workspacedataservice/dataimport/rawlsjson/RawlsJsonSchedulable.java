package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.RAWLSJSON;

import java.io.Serializable;
import java.util.Map;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;

public class RawlsJsonSchedulable extends Schedulable {
  public RawlsJsonSchedulable(
      String name, String description, Map<String, Serializable> arguments) {
    super(RAWLSJSON.name(), name, RawlsJsonQuartzJob.class, description, arguments);
  }
}
