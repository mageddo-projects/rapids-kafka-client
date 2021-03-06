package com.mageddo.kafka.client;

import java.time.Duration;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class DefaultConsumingConfig<K, V> implements ConsumingConfig<K, V> {

  public static final Duration DEFAULT_POLL_INTERVAL = Duration.ZERO;
  public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(2500);
  public static final RetryPolicy DEFAULT_RETRY_STRATEGY = RetryPolicy
      .builder()
      .maxTries(0)
      .delay(Duration.ZERO)
      .build();

  @NonNull
  @Builder.Default
  private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

  @NonNull
  @Builder.Default
  private Duration pollInterval = DEFAULT_POLL_INTERVAL;

  @NonNull
  @Builder.Default
  private RetryPolicy retryPolicy = DEFAULT_RETRY_STRATEGY;

  private RecoverCallback<K, V> recoverCallback;

  private ConsumeCallback<K, V> callback;

  private BatchConsumeCallback<K, V> batchCallback;

}
