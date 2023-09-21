package org.databiosphere.workspacedataservice.cache;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheLogger implements CacheEventListener<Object, Object> {
  private final Logger LOGGER = LoggerFactory.getLogger(CacheLogger.class);

  @Override
  public void onEvent(CacheEvent<?, ?> cacheEvent) {
    LOGGER.debug(
        "Key: {} | EventType: {} | Old value: {} | New value: {}",
        cacheEvent.getKey(),
        cacheEvent.getType(),
        cacheEvent.getOldValue(),
        cacheEvent.getNewValue());
  }
}
