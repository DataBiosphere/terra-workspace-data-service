package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;

import java.util.regex.Pattern;

/**
 * A collection of methods useful for working with SQL.
 */
public class SqlUtils {

    private static final Pattern DISALLOWED_CHARS_PATTERN = Pattern.compile("[^a-z0-9\\-_ ]", Pattern.CASE_INSENSITIVE);

    private static boolean containsDisallowedSqlCharacter(String name) {
        return name == null || DISALLOWED_CHARS_PATTERN.matcher(name).find();
    }

    public static String validateSqlString(String name, InvalidNameException.NameType nameType) {
        if (containsDisallowedSqlCharacter(name)) {
            throw new InvalidNameException(nameType);
        }
        return name;
    }

    public static String sanitizeSqlString(String input) {
        return DISALLOWED_CHARS_PATTERN.matcher(input).replaceAll("");
    }

}
