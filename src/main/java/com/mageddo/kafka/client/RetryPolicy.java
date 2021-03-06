package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class RetryPolicy {

  @Builder.Default
  private int maxTries = 0;

  @NonNull
  private Duration delay;

  private Duration maxDelay;

  private Collection<Class<? extends Throwable>> retryableExceptions;

  public Duration calcMaxTotalWaitTime(){
    if(this.delay.equals(Duration.ZERO) || this.maxTries == 0){
      return Duration.ZERO;
    }
    return Duration.ofMillis(this.delay.toMillis() * maxTries);
  }

  public RetryPolicy copy(){
    return RetryPolicy
        .builder()
        .delay(this.delay)
        .maxTries(this.maxTries)
        .retryableExceptions(new ArrayList<>(retryableExceptions))
        .build();
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
