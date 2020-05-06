package com.mageddo.kafka.client;

import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Fallback;

import lombok.Builder;
import lombok.Value;

import static com.mageddo.kafka.client.RetryPolicyConverter.retryPolicyToFailSafeRetryPolicy;

@Slf4j
@Value
@Builder
public class Retrier {

  private RetryPolicy retryPolicy;
  private Callback onRetry;
  private Callback onExhausted;

  public void run(final Callback run) {
    Failsafe
        .with(
            Fallback.ofAsync(ctx -> {
              if(log.isTraceEnabled()){
                log.trace("status=onExhausted, ctx={}", ctx);
              }
              onExhausted.call();
              return null;
            }),
            retryPolicyToFailSafeRetryPolicy(this.retryPolicy)
                .onRetry(ctx -> {
                  if(log.isTraceEnabled()){
                    log.trace("status=onRetry, ctx={}", ctx);
                  }
                  this.onRetry.call();
                })
                .handle(new ArrayList<>(this.retryPolicy.getRetryableExceptions()))
        )
        .run(ctx -> {
          if(log.isTraceEnabled()){
            log.trace("status=onRun, ctx={}", ctx);
          }
          run.call();
        });
  }
}
