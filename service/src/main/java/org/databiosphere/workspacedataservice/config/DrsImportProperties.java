package org.databiosphere.workspacedataservice.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "drs")
public class DrsImportProperties {
  private List<String> allowedHosts = new ArrayList<>();

  public List<String> getAllowedHosts() {
    return allowedHosts;
  }

  public void setAllowedHosts(List<String> allowedHosts) {
    this.allowedHosts = allowedHosts;
  }
}
