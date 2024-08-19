package org.databiosphere.workspacedataservice.workspace;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataTableTypeInspectorConfig {

  // in the control plane, use RawlsDataTableTypeInspector. It will query Rawls for information
  // about the workspace and return the appropriate data table type based on the Rawls response.
  @Bean
  @ControlPlane
  DataTableTypeInspector rawlsDataTableTypeInspector(RawlsClient rawlsClient) {
    return new RawlsDataTableTypeInspector(rawlsClient);
  }

  // in the data plane, use WdsDataTableTypeInspector, which is hardcoded to say data tables are
  // powered by WDS.
  @Bean
  @DataPlane
  DataTableTypeInspector wdsDataTableTypeInspector() {
    return new WdsDataTableTypeInspector();
  }
}
