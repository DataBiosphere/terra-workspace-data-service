package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class WorkspaceDataServiceApplication {

	@Bean
	public ObjectMapper objectMapper() {
		return JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
	}

	@Bean
	@Qualifier("streamingDs")
	@ConfigurationProperties("streaming.query")
	public DataSource streamingDs(){
		DataSource ds = DataSourceBuilder.create().build();
		HikariDataSource hikariDataSource = (HikariDataSource) ds;
		//https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
		hikariDataSource.setAutoCommit(false);
		return hikariDataSource;
	}

	@Bean
	@Qualifier("streamingDs")
	public NamedParameterJdbcTemplate templateForStreaming(@Qualifier("streamingDs") DataSource ds,
														   @Value("${twds.streaming.fetch.size:50}") int fetchSize){
		NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(ds);
		//https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
		jdbcTemplate.getJdbcTemplate().setFetchSize(fetchSize);
		return jdbcTemplate;
	}


	// CORS: allow Ajax requests from anywhere for all endpoints.
	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurer() {
			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**").allowedOrigins("*");
			}
		};
	}

	public static void main(String[] args) {
		SpringApplication.run(WorkspaceDataServiceApplication.class, args);
	}
}
