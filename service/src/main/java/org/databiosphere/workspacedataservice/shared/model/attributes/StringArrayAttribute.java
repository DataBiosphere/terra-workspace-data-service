package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;

public class StringArrayAttribute extends ArrayAttribute<StringAttribute> {
  StringArrayAttribute(List<StringAttribute> value) {
    super(value);
  }
}
