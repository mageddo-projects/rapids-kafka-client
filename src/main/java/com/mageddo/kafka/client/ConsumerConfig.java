package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

@Data
@Builder(toBuilder = true, buildMethodName = "$build")
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfig<K, V> implements ConsumerCreateConfig<K, V>, ConsumingConfig<K, V> {

  private final Map<String, Object> props = new HashMap<>();

  @Builder.Default
  private int consumers = 1;

  @NonNull
  private Collection<String> topics;

  @NonNull
  @Builder.Default
  private Duration timeout = DefaultConsumingConfig.DEFAULT_POLL_TIMEOUT;

  @NonNull
  @Builder.Default
  private Duration interval = DefaultConsumingConfig.FPS_30_DURATION;

  @NonNull
  @Builder.Default
  private RetryPolicy retryPolicy = DefaultConsumingConfig.DEFAULT_RETRY_STRATEGY;

  private RecoverCallback<K, V> recoverCallback;

  private ConsumeCallback<K, V> callback;

  private BatchConsumeCallback<K, V> batchCallback;

  public ConsumerConfig<K, V> prop(String k, Object v) {
    this.props.put(k, v);
    return this;
  }

  public ConsumerConfig<K, V> copy() {
    return ConsumerConfigBuilder.class.cast(this.toBuilder())
        .build();
  }

  public Map<String, Object> props() {
    this.prop(ENABLE_AUTO_COMMIT_CONFIG, false);
    this.prop(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Collections.unmodifiableMap(this.props);
  }

  public static class ConsumerConfigBuilder<K, V> {

    private Map<String, Object> props = new HashMap<>();

    public ConsumerConfigBuilder<K, V> topics(String... topics) {
      this.topics = Arrays.asList(topics);
      return this;
    }

    public ConsumerConfigBuilder<K, V> topics(Collection<String> topics) {
      this.topics = topics;
      return this;
    }

    public ConsumerConfigBuilder<K, V> prop(String key, Object value) {
      this.props.put(key, value);
      return this;
    }

    public ConsumerConfig<K, V> build() {
      final ConsumerConfig<K, V> config = $build();
      this.props.forEach(config::prop);
      return config;
    }
  }
}
