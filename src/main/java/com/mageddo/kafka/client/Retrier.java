package com.mageddo.kafka.client;

import java.util.ArrayList;
import java.util.function.Consumer;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Fallback;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.kafka.client.RetryPolicyConverter.retryPolicyToFailSafeRetryPolicy;

@Slf4j
@Value
@Builder
public class Retrier {

  @NonNull
  private RetryPolicy retryPolicy;
  private Callback onRetry;
  private Consumer<Throwable> onExhausted;

  public void run(final Callback run) {
    Failsafe
        .with(
            Fallback.ofAsync(ctx -> {
              if(log.isTraceEnabled()){
                log.trace("status=onExhausted, ctx={}", ctx);
              }
              onExhausted.accept(ctx.getLastFailure());
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
