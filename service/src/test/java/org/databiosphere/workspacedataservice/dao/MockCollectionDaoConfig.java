package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockCollectionDaoConfig {
  @Bean
  @Profile("mock-collection-dao")
  @Primary
  CollectionDao mockCollectionDao(@SingleTenant WorkspaceId workspaceId) {
    return new MockCollectionDao(workspaceId);
  }
}
