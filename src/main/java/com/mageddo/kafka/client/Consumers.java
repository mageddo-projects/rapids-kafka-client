package com.mageddo.kafka.client;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.util.Collections;

@UtilityClass
public class Consumers {

  public static <K, V> void consume(ConsumerConfig<K, V> consumerConfig) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(consumerConfig);
  }

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
      String topic,
      ConsumeCallback<K, V> consumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .copy()
            .topics(Collections.singletonList(topic))
            .callback(consumeCallback)
            .recoverCallback(recoverCallback)
    );
  }

  public static <K, V> void batchConsume(
      ConsumerConfig<K, V> consumerConfig,
      BatchConsumeCallback<K, V> batchConsumeCallback
  ) {
    batchConsume(consumerConfig, batchConsumeCallback, null);
  }

  public static <K, V> void batchConsume(
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

  public static <K, V> void batchConsume(
      ConsumerConfig<K, V> consumerConfig,
      String topic,
      BatchConsumeCallback<K, V> batchConsumeCallback,
      RecoverCallback<K, V> recoverCallback
  ) {
    final ConsumerFactory<K, V> consumerFactory = new ConsumerFactory<>();
    consumerFactory.consume(
        consumerConfig
            .copy()
            .topics(Collections.singletonList(topic))
            .batchCallback(batchConsumeCallback)
            .recoverCallback(recoverCallback)
    );
  }

  /**
   * wait until all threads terminate to end the program
   */
  @SneakyThrows
  public static void waitFor(){
    Thread.currentThread().join();
  }
}
