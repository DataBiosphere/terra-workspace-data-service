package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.NonNull;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.sql.DataSource;

@SpringBootApplication
public class WorkspaceDataServiceApplication {

	@Bean
	public ObjectMapper objectMapper() {
		return JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
				.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
				.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
				.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
				.findAndAddModules().build();
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
	public NamedParameterJdbcTemplate templateForStreaming(@Qualifier("streamingDs") DataSource ds,
			@Value("${twds.streaming.fetch.size:5000}") int fetchSize) {
		NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(ds);
		// https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
		jdbcTemplate.getJdbcTemplate().setFetchSize(fetchSize);
		return jdbcTemplate;
	}

	// CORS: allow Ajax requests from anywhere for all endpoints.
	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurer() {
			@Override
			public void addCorsMappings(@NonNull CorsRegistry registry) {
				registry.addMapping("/**").allowedOrigins("*");
			}

			@Override
			public void configureAsyncSupport(@NonNull AsyncSupportConfigurer configurer) {
				configurer.setDefaultTimeout(-1);
			}
		};
	}

	public static void main(String[] args) {
		SpringApplication.run(WorkspaceDataServiceApplication.class, args);
	}
}
