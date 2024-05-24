package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;

public class JsonArrayAttribute extends ArrayAttribute<JsonAttribute> {
  JsonArrayAttribute(List<JsonAttribute> value) {
    super(value);
  }
}
