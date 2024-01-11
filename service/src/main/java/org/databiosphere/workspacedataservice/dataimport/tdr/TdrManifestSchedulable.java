package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.TDRMANIFEST;

import java.io.Serializable;
import java.util.Map;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;

public class TdrManifestSchedulable extends Schedulable {

  public TdrManifestSchedulable(
      String name, String description, Map<String, Serializable> arguments) {
    super(TDRMANIFEST.name(), name, TdrManifestQuartzJob.class, description, arguments);
  }
}
