package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class RelationArrayAttribute extends ArrayAttribute<RelationAttribute> {
  RelationArrayAttribute(List<RelationAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_RELATION;
  }
}
