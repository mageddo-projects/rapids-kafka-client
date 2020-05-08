package com.mageddo.kafka.client;

import java.util.Collections;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Consumers {

  public static <K, V> ConsumerFactory<K, V> consume(ConsumerConfig<K, V> consumerConfig) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(consumerConfig);
    return consumerFactory;
  }

  public static <K, V> ConsumerFactory<K, V> consume(
      ConsumerConfig<K, V> consumerConfig,
      ConsumeCallback<K, V> consumeCallback
  ) {
    return consume(consumerConfig, consumeCallback, null);
  }

  public static <K, V> ConsumerFactory<K, V> consume(
      ConsumerConfig<K, V> consumerConfig,
      ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .toBuilder()
            .callback(consumeCallback)
            .recoverCallback(recoverCallback)
            .build()
    );
    return consumerFactory;
  }

  public static <K, V> ConsumerFactory<K, V> consume(
      ConsumerConfig<K, V> consumerConfig,
      String topic,
      ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .toBuilder()
            .topics(Collections.singletonList(topic))
            .callback(consumeCallback)
            .recoverCallback(recoverCallback)
            .build()
    );
    return consumerFactory;
  }

  public static <K, V> ConsumerFactory<K, V> batchConsume(
      ConsumerConfig<K, V> consumerConfig,
      BatchConsumeCallback<K, V> batchConsumeCallback
  ) {
    return batchConsume(consumerConfig, batchConsumeCallback, null);
  }

  public static <K, V> ConsumerFactory<K, V> batchConsume(
      ConsumerConfig<K, V> consumerConfig,
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .toBuilder()
            .batchCallback(batchConsumeCallback)
            .recoverCallback(recoverCallback)
            .build()
    );
    return consumerFactory;
  }

  public static <K, V> ConsumerFactory<K, V> batchConsume(
      ConsumerConfig<K, V> consumerConfig,
      String topic,
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .toBuilder()
            .topics(Collections.singletonList(topic))
            .batchCallback(batchConsumeCallback)
            .recoverCallback(recoverCallback)
            .build()
    );
    return consumerFactory;
  }

  /**
   * wait until all threads terminate to end the program
   */
  @SneakyThrows
  public static void waitFor() {
    Thread.currentThread()
        .join();
  }
}
