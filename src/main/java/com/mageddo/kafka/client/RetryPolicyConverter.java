package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;

public class RetryPolicyConverter {
  public static net.jodah.failsafe.RetryPolicy<?> retryPolicyToFailSafeRetryPolicy(RetryPolicy retryPolicy) {
    final net.jodah.failsafe.RetryPolicy<?> failSafeRetryPolicy = new net.jodah.failsafe.RetryPolicy<>();
    failSafeRetryPolicy.withMaxRetries(retryPolicy.getMaxTries());
    if(retryPolicy.getMaxDelay() != null && !retryPolicy.getMaxDelay().equals(Duration.ZERO)){
        failSafeRetryPolicy.withDelay(retryPolicy.getDelay());
    }

    if(retryPolicy.getMaxDelay() != null){
      failSafeRetryPolicy.withMaxDuration(retryPolicy.getMaxDelay());
    }
    if(!retryPolicy.getRetryableExceptions().isEmpty()){
        failSafeRetryPolicy.handle(new ArrayList<>(retryPolicy.getRetryableExceptions()));
    }
    return failSafeRetryPolicy;
  }
}
