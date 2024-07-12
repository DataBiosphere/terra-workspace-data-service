package org.databiosphere.workspacedataservice.security;

import org.databiosphere.workspacedataservice.sam.BearerTokenFilter;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.JobService;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

/** Bean setup for Spring Security */
@Configuration
@EnableWebSecurity
public class WebSecurityConfig {

  @Bean
  public BearerTokenFilter bearerTokenFilter() {
    return new BearerTokenFilter();
  }

  // Prevent the BearerTokenFilter from being automatically applied by Spring Boot.
  // Since BearerTokenFilter is a bean, it would be picked up both by Spring Boot and by
  // Spring Security and executed twice. We only want it to be used by Spring Security.
  // See:
  // https://docs.spring.io/spring-security/reference/servlet/architecture.html#adding-custom-filter
  @Bean
  public FilterRegistrationBean<BearerTokenFilter> bearerFilterRegistration(
      BearerTokenFilter filter) {
    FilterRegistrationBean<BearerTokenFilter> registration = new FilterRegistrationBean<>(filter);
    registration.setEnabled(false);
    return registration;
  }

  // Sam-powered authentication provider for use with Spring Security
  @Bean
  public AuthenticationProvider samAuthenticationProvider() {
    return new SamAuthenticationProvider();
  }

  // Sam-powered servlet filter for use with Spring Security
  @Bean
  public SamWorkspaceActionsFilter samActionsFilter(
      SamAuthorizationDaoFactory samAuthorizationDaoFactory,
      CollectionService collectionService,
      JobService jobService) {
    return new SamWorkspaceActionsFilter(samAuthorizationDaoFactory, collectionService, jobService);
  }

  // Prevent the SamWorkspaceActionsFilter from being automatically applied by Spring Boot.
  // Since SamWorkspaceActionsFilter is a bean, it would be picked up both by Spring Boot and by
  // Spring Security and executed twice. We only want it to be used by Spring Security.
  // See:
  // https://docs.spring.io/spring-security/reference/servlet/architecture.html#adding-custom-filter
  @Bean
  public FilterRegistrationBean<SamWorkspaceActionsFilter> samActionsFilterRegistration(
      SamWorkspaceActionsFilter filter) {
    FilterRegistrationBean<SamWorkspaceActionsFilter> registration =
        new FilterRegistrationBean<>(filter);
    registration.setEnabled(false);
    return registration;
  }

  // Configure Spring Security:
  //   * use the Sam-powered servlet filter
  //   * use the Sam-powered authentication provider
  //   * define which endpoints require which levels of security
  @Bean
  public SecurityFilterChain securityFilterChain(
      HttpSecurity http,
      AuthenticationProvider authenticationProvider,
      SamWorkspaceActionsFilter samActionsFilter,
      BearerTokenFilter bearerTokenFilter)
      throws Exception {

    // TODO: disable any other Spring Security features we don't need/want
    http.httpBasic(AbstractHttpConfigurer::disable);
    http.csrf(AbstractHttpConfigurer::disable);

    // use our custom authentication provider
    http.authenticationProvider(authenticationProvider);

    // add the Sam filter to the chain. Ensure it runs first, so we don't execute other
    // authn we don't need.
    http.addFilterBefore(samActionsFilter, BasicAuthenticationFilter.class);

    // add the bearer token filter to the chain, before the sam filter
    http.addFilterBefore(bearerTokenFilter, SamWorkspaceActionsFilter.class);

    // define endpoint:role requirements
    http.authorizeHttpRequests(
        (authz) ->
            authz
                // public endpoints
                .requestMatchers(
                    "/capabilities/**",
                    "/status/**",
                    "/version",
                    "/swagger/**",
                    "/webjars/**",
                    "/prometheus/**")
                .permitAll()
                // temporary hack to support the v0.2 version of list-instances
                .requestMatchers("/instances/v0.2")
                .permitAll()
                // else, if this is a GET we only need read permissions
                .requestMatchers(HttpMethod.GET)
                .hasRole("read")
                // and any other requests - e.g. POST, PATCH, PUT, DELETE require write permission
                .anyRequest()
                .hasRole("write"));

    return http.build();
  }
}
