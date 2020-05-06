package com.mageddo.kafka.client;

import java.util.Collection;
import java.util.Map;

public interface ConsumerCreateConfig<K, V> {

  Map<String, Object> getProps();

  Collection<String> getTopics();

  ConsumerCreateConfig<K, V> withProp(String k, Object v);
}
