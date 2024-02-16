package org.databiosphere.workspacedataservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Top-level ConfigurationProperties class representing the "twds" hierarchy of properties. */
@Configuration
@ConfigurationProperties(prefix = "twds")
public class TwdsProperties {
  DataImportProperties dataImport;
  InstanceProperties instance;
  String startupToken;
  TenancyProperties tenancy;

  public DataImportProperties getDataImport() {
    return dataImport;
  }

  public void setDataImport(DataImportProperties dataImport) {
    this.dataImport = dataImport;
  }

  public InstanceProperties getInstance() {
    return instance;
  }

  public void setInstance(InstanceProperties instance) {
    this.instance = instance;
  }

  public String getStartupToken() {
    return startupToken;
  }

  public void setStartupToken(String startupToken) {
    this.startupToken = startupToken;
  }

  public TenancyProperties getTenancy() {
    return tenancy;
  }

  public void setTenancy(TenancyProperties tenancy) {
    this.tenancy = tenancy;
  }
}
