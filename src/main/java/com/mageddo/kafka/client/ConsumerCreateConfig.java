package com.mageddo.kafka.client;

import java.util.Collection;
import java.util.Map;

public interface ConsumerCreateConfig<K, V> {

  Map<String, Object> props();

  Collection<String> topics();

  ConsumerCreateConfig<K, V> prop(String k, Object v);

  /**
   * How many threads create to consume the topic, you can set this value specifically to {@link Integer#MIN_VALUE}
   * to disable the consumer, it's also useful for testing when you don't want tests to consume a real kafka
   */
  int consumers();
}
