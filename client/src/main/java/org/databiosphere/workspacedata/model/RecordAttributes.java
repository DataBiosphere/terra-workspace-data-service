package org.databiosphere.workspacedata.model;

import java.util.HashMap;

/**
 * The WDS Java client uses this class instead of the code generated by openapi-generator.
 *
 * <p>See client/swagger.gradle for the implementation of this customization, specifically the
 * generateAliasAsModel and schemaMappings settings.
 *
 * <p>openapi-generator has bugs around the use of generateAliasAsModel, and the RecordAttributes
 * model needs generateAliasAsModel to work.
 * https://github.com/OpenAPITools/openapi-generator/issues/10848 is the best bug to start with if
 * you want to look.
 *
 * <p>RecordAttributes, as defined by service/src/main/resources/static/swagger/openapi-docs.yaml,
 * is nothing more than a map of String to Object. But, openapi-generator creates code that either
 * uses Map<String, Object> directly or uses RecordAttributes but has extra layers of abstraction
 * piled on top of RecordAttributes.
 *
 * <p>Here, we simplify and declare that RecordAttributes is an extension of HashMap.
 */
public class RecordAttributes extends HashMap<String, Object> {}
