package org.databiosphere.workspacedataservice.service;

/**
 * Marker interface that denotes a StreamingWriteHandler which runs in two passes. The first pass
 * will upsert base attributes, and the second pass will upsert relation attributes. This allows for
 * data streams where earlier records contain relations to later records.
 */
public interface TwoPassStreamingWriteHandler extends StreamingWriteHandler {}
