package com.mageddo.kafka.client;

import java.util.Collection;
import java.util.Map;

public interface ConsumerCreateConfig<K, V> {

  /**
   * Props set at the created {@link org.apache.kafka.clients.consumer.Consumer} instance
   */
  Map<String, Object> props();

  Collection<String> topics();

  ConsumerCreateConfig<K, V> prop(String k, Object v);

  /**
   * How many threads create to consume the topic, you can set this value specifically to {@link Integer#MIN_VALUE}
   * to disable the consumer, it's also useful for testing when you don't want tests to consume a real kafka
   */
  int consumers();

  /**
   * Lambda function used to create the consumer instance, it's useful for component or integrated tests,
   * for example, mocking kafka by using {@link org.apache.kafka.clients.consumer.MockConsumer} instead of
   * {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
   * {@link ConsumerController#defaultConsumerSupplier()} is used by default.
   */
  ConsumerSupplier<K, V> consumerSupplier();
}
