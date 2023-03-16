package org.databiosphere.workspacedataservice.dao;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSource mainDb() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(mainDb());
    }

    @Bean
    @Qualifier("streamingDs")
    @ConfigurationProperties("streaming.query")
    public DataSource streamingDs() {
        DataSource ds = DataSourceBuilder.create().build();
        HikariDataSource hikariDataSource = (HikariDataSource) ds;
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        hikariDataSource.setAutoCommit(false);
        return hikariDataSource;
    }

    @Bean
    @Qualifier("streamingDs")
    public NamedParameterJdbcTemplate templateForStreaming(@Qualifier("streamingDs") DataSource ds,
                                                           @Value("${twds.streaming.fetch.size:5000}") int fetchSize) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(ds);
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        jdbcTemplate.getJdbcTemplate().setFetchSize(fetchSize);
        return jdbcTemplate;
    }

}
