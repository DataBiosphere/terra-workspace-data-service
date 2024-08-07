package org.databiosphere.workspacedataservice.dao;

import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.dao.converters.CollectionIdReadingConverter;
import org.databiosphere.workspacedataservice.dao.converters.CollectionIdWritingConverter;
import org.databiosphere.workspacedataservice.dao.converters.WorkspaceIdReadingConverter;
import org.databiosphere.workspacedataservice.dao.converters.WorkspaceIdWritingConverter;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;

/** Bean definitions for Spring Data repositories */
@Configuration
public class DataRepositoryConfig extends AbstractJdbcConfiguration {

  /**
   * Custom converters for CollectionId and WorkspaceId wrappers. These allow Spring Data to use
   * CollectionId and WorkspaceId anywhere it would otherwise use UUID.
   *
   * @return WDS's custom converters
   */
  @Override
  protected List<?> userConverters() {
    return Arrays.asList(
        new CollectionIdWritingConverter(),
        new CollectionIdReadingConverter(),
        new WorkspaceIdWritingConverter(),
        new WorkspaceIdReadingConverter());
  }
}
