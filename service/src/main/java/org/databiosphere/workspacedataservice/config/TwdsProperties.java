package org.databiosphere.workspacedataservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Top-level ConfigurationProperties class representing the "twds" hierarchy of properties. */
@Configuration
@ConfigurationProperties(prefix = "twds")
public class TwdsProperties {
  private DataImportProperties dataImport;
  private InstanceProperties instance;
  private String startupToken;
  private TenancyProperties tenancy;

  public DataImportProperties getDataImport() {
    return dataImport;
  }

  @Bean
  public DataImportProperties dataImportProperties() {
    return getDataImport();
  }

  public void setDataImport(DataImportProperties dataImport) {
    this.dataImport = dataImport;
  }

  public InstanceProperties getInstance() {
    return instance;
  }

  @Bean
  public InstanceProperties instanceProperties() {
    return getInstance();
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

  @Bean
  public TenancyProperties tenancyProperties() {
    return getTenancy();
  }

  public void setTenancy(TenancyProperties tenancy) {
    this.tenancy = tenancy;
  }
}
