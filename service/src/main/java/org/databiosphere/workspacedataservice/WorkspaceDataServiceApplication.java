package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class WorkspaceDataServiceApplication {

	@Bean
	public ObjectMapper objectMapper(){
		return JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
	}
	public static void main(String[] args) {
		SpringApplication.run(WorkspaceDataServiceApplication.class, args);
	}
}
