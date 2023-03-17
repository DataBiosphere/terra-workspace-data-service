package org.databiosphere.workspacedataservice.dao;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest(classes = DataSourceConfig.class)
public class PostgresInstanceDaoTest {

    @Autowired
    NamedParameterJdbcTemplate namedTemplate;

//    @Test
//    void workspaceIDNotProvidedNoExceptionThrown() {
//        assertDoesNotThrow(() -> new PostgresInstanceDao(namedTemplate, "UNDEFINED"));
//    }

}
