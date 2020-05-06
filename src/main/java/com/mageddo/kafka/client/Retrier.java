package com.mageddo.kafka.client;

import java.util.ArrayList;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Fallback;

import lombok.Builder;
import lombok.Value;

import static com.mageddo.kafka.client.RetryPolicyConverter.retryPolicyToFailSafeRetryPolicy;

@Value
@Builder
public class Retrier {

  private RetryPolicy retryPolicy;
  private Callback onRetry;
  private Callback onExhausted;

  public void run(final Callback run) {
    Failsafe
        .with(
            Fallback.ofAsync(it -> {
              run.call();
              return null;
            }),
            retryPolicyToFailSafeRetryPolicy(this.retryPolicy)
                .onRetry(it -> this.onRetry.call())
                .handle(new ArrayList<>(this.retryPolicy.getRetryableExceptions()))
        )
        .run(ctx -> this.onExhausted.call());
  }
}
