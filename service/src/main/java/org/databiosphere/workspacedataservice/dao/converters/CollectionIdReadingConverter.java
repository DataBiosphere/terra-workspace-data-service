package org.databiosphere.workspacedataservice.dao.converters;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

@ReadingConverter
public class CollectionIdReadingConverter implements Converter<UUID, CollectionId> {
  /**
   * @param source
   * @return
   */
  @Override
  public CollectionId convert(@NotNull UUID source) {
    return CollectionId.of(source);
  }
}
