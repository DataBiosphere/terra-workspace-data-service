package org.databiosphere.workspacedataservice.dao.converters;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

@WritingConverter
public class CollectionIdWritingConverter implements Converter<CollectionId, UUID> {

  @Override
  public UUID convert(CollectionId source) {
    return source.id();
  }
}
