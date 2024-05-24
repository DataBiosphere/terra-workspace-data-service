package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;

public class RelationArrayAttribute extends ArrayAttribute<RelationAttribute> {
  RelationArrayAttribute(List<RelationAttribute> value) {
    super(value);
  }
}
