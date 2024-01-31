package org.databiosphere.workspacedataservice.service.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class AttributeSchemaTest {
  @Test
  void testAlternateConstructors() {
    assertEquals(
        new AttributeSchema("attr", "STRING", RecordType.valueOf("otherAttr")),
        new AttributeSchema("attr", DataTypeMapping.STRING, RecordType.valueOf("otherAttr")));

    assertEquals(
        new AttributeSchema("attr", "STRING", null), new AttributeSchema("attr", "STRING"));

    assertEquals(
        new AttributeSchema("attr", "STRING", null),
        new AttributeSchema("attr", DataTypeMapping.STRING));

    assertEquals(new AttributeSchema("attr", (String) null, null), new AttributeSchema("attr"));
  }
}
