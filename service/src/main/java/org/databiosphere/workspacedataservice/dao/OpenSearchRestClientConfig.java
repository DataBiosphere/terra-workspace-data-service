package org.databiosphere.workspacedataservice.dao;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenSearchRestClientConfig {

    @Bean
    public OpenSearchClient getClient() {
        // TODO: get from application.properties
        final HttpHost host = new HttpHost("localhost", 9200, "http");
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        // TODO: get from application.properties
        //Only for demo purposes. Don't specify your credentials in code.
        credentialsProvider.setCredentials(new AuthScope(host),
                new UsernamePasswordCredentials("admin", "admin"));

        // TODO: enable SSL/TLS?
        //Initialize the client with SSL and TLS disabled
        final RestClient restClient = RestClient.builder(host).
                setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)).build();

        final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new OpenSearchClient(transport);
    }

}
