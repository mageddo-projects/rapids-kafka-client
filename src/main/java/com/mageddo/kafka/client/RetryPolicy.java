package com.mageddo.kafka.client;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

@Value
@Builder
public class RetryPolicy {

  @Builder.Default
  private int maxTries = 2;

  private Duration delay;

  private Duration maxDelay;

  private Collection<Class<? extends Throwable>> retryableExceptions;

  public Duration calcMaxTotalWaitTime(){
    return Duration.ofMillis(this.delay.toMillis() * maxTries);
  }

  public static class RetryPolicyBuilder {

    public RetryPolicyBuilder() {
      this.retryableExceptions = new LinkedHashSet<>();
      this.retryableExceptions.add(Exception.class);
    }

      /**
       * Replace all before handled exceptions by the informed
     */
    public RetryPolicyBuilder handleExceptions(Class<? extends Throwable>... exceptions) {
      this.retryableExceptions.clear();
      this.retryableExceptions.addAll(Arrays.asList(exceptions));
      return this;
    }

    public RetryPolicyBuilder addRetryableException(Class<? extends Throwable> e) {
      this.retryableExceptions.add(e);
      return this;
    }
  }
}
