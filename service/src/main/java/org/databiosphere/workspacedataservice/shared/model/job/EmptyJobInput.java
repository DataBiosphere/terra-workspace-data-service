package org.databiosphere.workspacedataservice.shared.model.job;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * used for Jobs that have no input arguments.
 *
 * <p>The explicit JsonSerialize is necessary here so Jackson knows to serialize this empty class.
 * Jackson will write it as {}. Alternately, we could set SerializationFeature.FAIL_ON_EMPTY_BEANS
 * to false, but that's a global change.
 */
@JsonSerialize
public class EmptyJobInput implements JobInput {}
