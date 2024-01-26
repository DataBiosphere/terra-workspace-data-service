package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.PFB;

import java.io.Serializable;
import java.util.Map;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;

public class PfbSchedulable extends Schedulable {

  public PfbSchedulable(String name, String description, Map<String, Serializable> arguments) {
    super(PFB.name(), name, PfbQuartzJob.class, description, arguments);
  }
}
