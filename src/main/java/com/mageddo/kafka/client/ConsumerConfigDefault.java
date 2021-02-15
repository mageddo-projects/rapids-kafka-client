package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.kafka.client.internal.ObjectsUtils.firstNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;


@Getter
@Builder(builderClassName = "Builder")
@Accessors(chain = true, fluent = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@EqualsAndHashCode
@AllArgsConstructor
public class ConsumerConfigDefault<K, V> implements ConsumerConfig<K, V> {

  public static final int CONSUMERS_NOT_SET = -42;

  /**
   * @see ConsumerCreateConfig#props()
   */
  private Map<String, Object> props;

  /**
   * @see ConsumerCreateConfig#consumers()
   */
  private int consumers;

  /**
   * @see ConsumerCreateConfig#topics()
   */
  @NonNull
  private Collection<String> topics;

  /**
   * @see ConsumingConfig#pollTimeout()
   */
  private Duration pollTimeout;

  /**
   * @see ConsumingConfig#pollInterval()
   */
  private Duration pollInterval;

  /**
   * @see ConsumingConfig#retryPolicy()
   */
  private RetryPolicy retryPolicy;

  /**
   * @see ConsumingConfig#recoverCallback() )
   */
  private RecoverCallback<K, V> recoverCallback;

  /**
   * @see ConsumingConfig#callback()
   */
  private ConsumeCallback<K, V> callback;

  /**
   * @see ConsumingConfig#batchCallback()
   */
  private BatchConsumeCallback<K, V> batchCallback;

  /**
   * @see ConsumerCreateConfig#consumerSupplier()
   */
  private ConsumerSupplier<K, V> consumerSupplier;

  public String groupId() {
    if (this.props.containsKey(GROUP_ID_CONFIG)) {
      return String.valueOf(this.props.get(GROUP_ID_CONFIG));
    }
    return null;
  }

  public ConsumerConfigDefault<K, V> prop(String k, Object v) {
    this.props.put(k, v);
    return this;
  }

  public Map<String, Object> props() {
    return Collections.unmodifiableMap(this.props);
  }

  public Builder<K, V> toBuilder() {
    return new Builder<K, V>()
        .consumers(this.consumers)
        .topics(new ArrayList<>(this.topics))
        .pollTimeout(this.pollTimeout)
        .pollInterval(this.pollInterval)
        .retryPolicy(this.retryPolicy != null ? this.retryPolicy.copy() : null)
        .recoverCallback(this.recoverCallback)
        .callback(this.callback)
        .batchCallback(this.batchCallback)
        .props(new HashMap<>(this.props))
        ;
  }

  public ConsumerController<K, V> consume() {
    return this.consume(this);
  }

  public ConsumerController<K, V> consume(ConsumerConfigDefault<K, V> consumerConfig) {
    final ConsumerController<K, V> consumerController = new ConsumerController<>();
    consumerController.consume(consumerConfig);
    return consumerController;
  }

  public ConsumerController<K, V> consume(ConsumeCallback<K, V> consumeCallback) {
    return this.consume(consumeCallback, null);
  }

  public ConsumerController<K, V> consume(ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback) {
    return this.consume(this
        .toBuilder()
        .callback(consumeCallback)
        .recoverCallback(recoverCallback)
        .build()
    );
  }

  public ConsumerController<K, V> consume(
      String topic,
      ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    return this.consume(this
        .toBuilder()
        .topics(Collections.singletonList(topic))
        .callback(consumeCallback)
        .recoverCallback(recoverCallback)
        .build()
    );
  }

  public ConsumerController<K, V> batchConsume(BatchConsumeCallback<K, V> batchConsumeCallback) {
    return this.batchConsume(batchConsumeCallback, null);
  }

  public ConsumerController<K, V> batchConsume(
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    return this.consume(this
        .toBuilder()
        .batchCallback(batchConsumeCallback)
        .recoverCallback(recoverCallback)
        .build()
    );
  }

  public ConsumerController<K, V> batchConsume(
      String topic,
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    return this.consume(this
        .toBuilder()
        .topics(Collections.singletonList(topic))
        .batchCallback(batchConsumeCallback)
        .recoverCallback(recoverCallback)
        .build()
    );
  }

  /**
   * wait for the all other threads terminate
   */
  @SneakyThrows
  public static void waitFor() {
    Thread.currentThread()
        .join();
  }

  public static <K, V> Builder<K, V> builderOf(ConsumerConfig<K, V> config) {
    return copyToBuilder(config, ConsumerConfigDefault.<K, V>builder().build());
  }

  public static <K, V> ConsumerConfigDefault<K, V> copy(ConsumerConfig<K, V> primary, ConsumerConfig<K, V> secondary) {
    return copyToBuilder(primary, secondary).build();
  }

  public static <K, V> Builder<K, V> copyToBuilder(ConsumerConfig<K, V> primary, ConsumerConfig<K, V> secondary) {
    final Builder<K, V> builder = ConsumerConfigDefault.builder();
    fillWith(secondary, builder);
    fillWith(primary, builder);
    return builder
        .callback(firstNonNull(primary.callback(), secondary.callback()))
        .batchCallback(firstNonNull(primary.batchCallback(), secondary.batchCallback()))
        .topics(
            primary.topics()
            .isEmpty() ? secondary.topics() : primary.topics()
        )
        .consumers(primary.consumers() != CONSUMERS_NOT_SET ? primary.consumers() : secondary.consumers())
        .recoverCallback(firstNonNull(primary.recoverCallback(), secondary.recoverCallback()))
        .retryPolicy(firstNonNull(primary.retryPolicy(), secondary.retryPolicy()))
        .consumerSupplier(firstNonNull(primary.consumerSupplier(), secondary.consumerSupplier()))
        .pollInterval(firstNonNull(primary.pollInterval(), secondary.pollInterval()))
        .pollTimeout(firstNonNull(primary.pollTimeout(), secondary.pollTimeout()));
  }

  private static <K, V> void fillWith(ConsumerConfig<K, V> source, Builder<K, V> target) {
    source
        .props()
        .forEach(target::prop);
  }

  @Slf4j
  public static class Builder<K, V> {

    public Builder() {
      this.props = new LinkedHashMap<>();
      this.consumers = CONSUMERS_NOT_SET;
      this.topics = Collections.EMPTY_LIST;
    }

    public Builder<K, V> consumers(int consumers) {
      if (this.consumers == Integer.MIN_VALUE) {
        log.info(
            "consumer was previously disabled by set it's value to {}, it won't be re enabled. groupId={}, topics={}",
            Integer.MIN_VALUE,
            this.props.get(GROUP_ID_CONFIG),
            this.topics
        );
        return this;
      }
      this.consumers = consumers;
      return this;
    }

    public Builder<K, V> topics(String... topics) {
      this.topics = Arrays.asList(topics);
      return this;
    }

    public Builder<K, V> topics(Collection<String> topics) {
      this.topics = topics;
      return this;
    }

    public Builder<K, V> prop(String key, Object value) {
      this.props.put(key, value);
      return this;
    }

    public ConsumerConfigDefault<K, V> build() {
      return new ConsumerConfigDefault<>(
          this.props,
          this.consumers,
          this.topics,
          this.pollTimeout,
          this.pollInterval,
          this.retryPolicy,
          this.recoverCallback,
          this.callback,
          this.batchCallback,
          this.consumerSupplier
      );
    }
  }

  @Override
  public String toString() {
    return String.format("ConsumerConfig(groupId=%s, topics=%s)", this.groupId(), this.topics);
  }
}
