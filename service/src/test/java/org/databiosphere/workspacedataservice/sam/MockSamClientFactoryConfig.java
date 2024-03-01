package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * Bean-creator for MockSamClientFactory for use in unit tests.
 *
 * <p>Unit tests which would otherwise require a Sam instance to be running can activate the
 * "mock-sam" Spring profile to use these mock implementations instead, which:
 *
 * <ul>
 *   <li>Always return true for all permission checks.
 *   <li>Never throw any Exceptions.
 * </ul>
 *
 * @see MockSamClientFactory
 * @see MockSamResourcesApi
 * @see MockSamUsersApi
 * @see MockSamGoogleApi
 * @see MockStatusApi
 */
@Configuration
public class MockSamClientFactoryConfig {

  /**
   * provide a {@link MockSamClientFactory} to unit tests marked with the "mock-sam" profile. marked
   * as {@code @Primary} here to ensure it overrides the {@link SamClientFactory} provided by the
   * runtime {@link SamConfig}.
   */
  @Bean
  @Profile("mock-sam")
  @Primary
  public SamClientFactory getMockSamClientFactory() {
    return new MockSamClientFactory();
  }
}
