package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlUtilsTest {

    @Test
    void testValidateSqlStringFailures() {
        List<String> badStrings = List.of(
                "); drop table users;",
                "$$foo.bar",
                "...",
                "&Q$(*^@$(*",
                "\\u0027\\u003b\\u0020\\u006f\\u0072\\u0020\\u0031\\u003d\\u0031\\u0020\\u0027\\u0027"
                );
        for (String badString : badStrings) {
            assertThrows(InvalidNameException.class, () -> {
                SqlUtils.validateSqlString(badString, InvalidNameException.NameType.RECORD_TYPE);
            }, "for input '" + badString + "'");
        }
    }

    @Test
    void testValidateSqlStringSuccesses() {
        List<String> goodStrings = List.of(
                "droptable",
                "where",
                "sleep",
                "in"
        );
        for (String goodString : goodStrings) {
            assertDoesNotThrow(() -> {
                SqlUtils.validateSqlString(goodString, InvalidNameException.NameType.RECORD_TYPE);
            }, "for input '" + goodString + "'");
        }
    }

}
