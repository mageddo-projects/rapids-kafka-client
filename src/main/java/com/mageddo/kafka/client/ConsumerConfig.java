package com.mageddo.kafka.client;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfig<K, V> implements ConsumerCreateConfig<K, V>, ConsumingConfig<K, V> {

  @Getter
  private final Map<String, Object> props = new HashMap<>();

  @NonNull
  private Collection<String> topics;

  @NonNull
  @Builder.Default
  private Duration timeout = ConsumingConfigDefault.DEFAULT_POLL_TIMEOUT;

  @NonNull
  @Builder.Default
  private Duration interval = ConsumingConfigDefault.FPS_30_DURATION;

  @NonNull
  @Builder.Default
  private RetryPolicy retryPolicy = ConsumingConfigDefault.DEFAULT_RETRY_STRATEGY;

  private RecoverCallback<K, V> recoverCallback;

  private ConsumeCallback<K, V> callback;

  private BatchConsumeCallback<K, V> batchCallback;

  public ConsumerConfig<K, V> withProp(String k, Object v) {
    this.props.put(k, v);
    return this;
  }

  public ConsumerConfig<K, V> copy(){
    return this.toBuilder().build();
  }
}
