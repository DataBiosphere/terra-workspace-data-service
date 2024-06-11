package org.databiosphere.workspacedataservice.recordsink;

/**
 * Utility to conditionally rename attributes that originate in a PFB or TDR snapshot; renaming
 * ensures that names are legal for consumption by Rawls. The renaming logic is not consistent
 * between PFB and TDR due to legacy business requirements. the logic here replicates what was
 * already implemented by Import Service.
 */
public class RawlsAttributePrefixer {

  // which prefixing strategy (pfb vs. tdr) should we use?
  public enum PrefixStrategy {
    PFB("pfb"),
    TDR("tdr"),
    NONE("none");

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

  /**
   * Conditionally prefix and rename inbound attributes for compatibility with Rawls.
   *
   * @param attributeName the inbound original attribute name
   * @param recordType record type containing this attribute (Rawls call this "entityType")
   * @return the potentially prefixed/renamed attribute
   */
  public String prefix(String attributeName, String recordType) {
    // Some attributes (such as import metadata) already have a prefix.
    // Do not add multiple prefixes.
    if (hasPrefix(attributeName)) {
      return attributeName;
    }
    return switch (prefixStrategy) {
      case PFB -> pfbPrefix(attributeName, recordType);
      case TDR -> tdrPrefix(attributeName, recordType);
      default -> attributeName; // TSV, JSON strategy:
    };
  }

  /**
   * Check if an attribute name includes a prefix.
   *
   * @param attributeName the inbound original attribute name
   * @return whether the attribute name has a prefix
   */
  private boolean hasPrefix(String attributeName) {
    return attributeName.contains(":");
  }

  /*
     Desired functionality:
     - if input is "name", return "pfb:${entityType}_name"
     - else, return "pfb:${input}"
  */
  private String pfbPrefix(String attributeName, String recordType) {
    if ("name".equals(attributeName)) {
      return "%s:%s_name".formatted(prefixStrategy.getPrefix(), recordType);
    }
    return "%s:%s".formatted(prefixStrategy.getPrefix(), attributeName);
  }

  /*
     Desired functionality:
     - if input is "name", return "tdr:name"
     - if input is "entityType", return "tdr:entityType"
     - if input is "${entityType}_id", return "tdr:${entityType}_id"
     - else, return input
  */
  private String tdrPrefix(String attributeName, String recordType) {
    if ("name".equals(attributeName)) {
      return "%s:name".formatted(prefixStrategy.getPrefix());
    }
    if ("entityType".equals(attributeName)) {
      return "%s:entityType".formatted(prefixStrategy.getPrefix());
    }
    if ("%s_id".formatted(recordType).equals(attributeName)) {
      return "%s:%s_id".formatted(prefixStrategy.getPrefix(), recordType);
    }
    return attributeName;
  }
}
