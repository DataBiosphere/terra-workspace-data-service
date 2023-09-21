package org.databiosphere.workspacedataservice.shared.model;

import java.text.Collator;
import java.util.Comparator;

public record AttributeComparator(String primaryKey) implements Comparator<String> {

  @Override
  public int compare(String o1, String o2) {
    if (o1.equals(primaryKey)) {
      return -1;
    }
    if (o2.equals(primaryKey)) {
      return 1;
    }
    Collator collator = Collator.getInstance();
    return collator.compare(o1, o2);
  }
}
