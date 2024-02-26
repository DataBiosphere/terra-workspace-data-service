package org.databiosphere.workspacedataservice.config;

import java.util.Optional;
import javax.annotation.Nullable;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Top-level ConfigurationProperties class representing the "twds" hierarchy of properties. */
@Configuration
@ConfigurationProperties(prefix = "twds")
public class TwdsProperties {
  private DataImportProperties dataImport;
  private TenancyProperties tenancy;
  private InstanceProperties instance;
  private String startupToken;

  @Bean
  public DataImportProperties dataImportProperties() {
    return dataImport;
  }

  // setter method allows spring to automatically populate this with the contents of the
  // twds.data-import section of the props file
  void setDataImport(DataImportProperties dataImport) {
    this.dataImport = dataImport;
  }

  /**
   * InstanceProperties are only returned if tenancy requires a WORKSPACE_ID environment variable,
   * and if that environment variable is correctly configured with a WorkspaceId.
   */
  @Bean
  @Nullable
  @ConditionalOnProperty(name = "twds.tenancy.require-env-workspace", havingValue = "true")
  public InstanceProperties instanceProperties() {
    return safeGetInstanceProperties().orElse(null);
  }

  /**
   * WorkspaceId is only returned if tenancy requires a WORKSPACE_ID environment variable, and if
   * that environment variable is correctly configured with a WorkspaceId.
   */
  @Bean
  @SingleTenant
  @ConditionalOnProperty(name = "twds.tenancy.require-env-workspace", havingValue = "true")
  public WorkspaceId workspaceId() {
    return safeGetInstanceProperties().map(InstanceProperties::workspaceId).orElse(null);
  }

  // setter method allows spring to automatically populate this with the contents of the
  // twds.instance section of the props file
  void setInstance(InstanceProperties instance) {
    this.instance = instance;
  }

  public String getStartupToken() {
    return startupToken;
  }

  // setter method allows spring to automatically populate this with the contents of the
  // twds.startup-token property from the props file
  void setStartupToken(String startupToken) {
    this.startupToken = startupToken;
  }

  @Bean
  public TenancyProperties tenancyProperties() {
    return tenancy;
  }

  // setter method allows spring to automatically populate this with the contents of the
  // twds.tenancy section of the props file
  void setTenancy(TenancyProperties tenancy) {
    this.tenancy = tenancy;
  }

  // only return InstanceProperties if it is correctly configured with a WorkspaceId
  private Optional<InstanceProperties> safeGetInstanceProperties() {
    if (instance != null && instance.hasValidWorkspaceId()) {
      return Optional.of(instance);
    }
    return Optional.empty();
  }
}
