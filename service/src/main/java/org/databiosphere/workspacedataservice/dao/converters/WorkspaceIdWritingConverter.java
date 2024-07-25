package org.databiosphere.workspacedataservice.dao.converters;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

@WritingConverter
public class WorkspaceIdWritingConverter implements Converter<WorkspaceId, UUID> {

  @Override
  public UUID convert(WorkspaceId source) {
    return source.id();
  }
}
