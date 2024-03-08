package org.databiosphere.workspacedataservice.recordsink;

/*
   Desired implementation:
    TDR
      name -> tdr:name
      entityType -> tdr:entityType
      ${entityType}_id -> tdr:${entityType}_id

    PFB
      name -> pfb:${entityType}_name
      * -> pfb:*
*/
public class RawlsAttributePrefixer {

  public enum PrefixStrategy {
    PFB("pfb"),
    TDR("tdr");

    private final String prefix;

    PrefixStrategy(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  private final PrefixStrategy prefixStrategy;

  public RawlsAttributePrefixer(PrefixStrategy prefixStrategy) {
    this.prefixStrategy = prefixStrategy;
  }

  public String prefix(String attributeName, String recordType) {
    if (this.prefixStrategy.equals(PrefixStrategy.PFB)) {
      return pfbPrefix(attributeName, recordType);
    }
    return tdrPrefix(attributeName, recordType);
  }

  private String pfbPrefix(String attributeName, String recordType) {
    return attributeName;
  }

  private String tdrPrefix(String attributeName, String recordType) {
    return attributeName;
  }
}
