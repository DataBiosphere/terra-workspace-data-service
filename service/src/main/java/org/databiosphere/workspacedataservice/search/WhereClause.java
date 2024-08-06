package org.databiosphere.workspacedataservice.search;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public record WhereClause(String sql, MapSqlParameterSource params) {}
