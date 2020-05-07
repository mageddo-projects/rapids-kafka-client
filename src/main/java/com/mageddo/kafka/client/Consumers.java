package com.mageddo.kafka.client;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Consumers {

  public static <K, V> void consume(
      ConsumerConfig<K, V> consumerConfig,
      ConsumeCallback<K, V> consumeCallback
  ) {
    consume(consumerConfig, consumeCallback, null);
  }

  public static <K, V> void consume(
      ConsumerConfig<K, V> consumerConfig,
      ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .copy()
            .callback(consumeCallback)
            .recoverCallback(recoverCallback)
    );
  }

  public static <K, V> void consume(
      ConsumerConfig<K, V> consumerConfig,
      BatchConsumeCallback<K, V> batchConsumeCallback
  ) {
    consume(consumerConfig, batchConsumeCallback, null);
  }

  public static <K, V> void consume(
      ConsumerConfig<K, V> consumerConfig,
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .copy()
            .batchCallback(batchConsumeCallback)
            .recoverCallback(recoverCallback)
    );
  }
}
