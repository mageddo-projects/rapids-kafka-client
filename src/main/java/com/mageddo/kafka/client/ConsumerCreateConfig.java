package com.mageddo.kafka.client;

import java.util.Collection;
import java.util.Map;

public interface ConsumerCreateConfig<K, V> {

  Map<String, Object> props();

  Collection<String> topics();

  ConsumerCreateConfig<K, V> prop(String k, Object v);

  /**
   * How many threads create for the topic
   */
  int consumers();
}
