package com.mageddo.kafka.client;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

import java.time.Duration;

@Value
@Builder
@Accessors(fluent = true)
public class DefaultConsumingConfig<K, V> implements ConsumingConfig<K, V> {

  public static final int FPS_30 = 1000 / 30;
  public static final Duration FPS_30_DURATION = Duration.ofMillis(FPS_30);
  public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
  public static final RetryPolicy DEFAULT_RETRY_STRATEGY = RetryPolicy
      .builder()
      .delay(Duration.ofSeconds(5))
      .build();

  @NonNull
  @Builder.Default
  private Duration timeout = DEFAULT_POLL_TIMEOUT;

  @NonNull
  @Builder.Default
  private Duration interval = FPS_30_DURATION;

  @NonNull
  @Builder.Default
  private RetryPolicy retryPolicy = DEFAULT_RETRY_STRATEGY;

  private RecoverCallback<K, V> recoverCallback;

  private ConsumeCallback<K, V> callback;

  private BatchConsumeCallback<K, V> batchCallback;

}
