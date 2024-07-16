package org.databiosphere.workspacedataservice.dao.converters;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

@ReadingConverter
public class WorkspaceIdReadingConverter implements Converter<UUID, WorkspaceId> {
  /**
   * @param source
   * @return
   */
  @Override
  public WorkspaceId convert(@NotNull UUID source) {
    return WorkspaceId.of(source);
  }
}
