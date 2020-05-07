package com.mageddo.kafka.client;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
@Accessors(fluent = true)
public class DefaultConsumerCreateConfig<K, V> implements ConsumerCreateConfig<K, V> {

  private Map<String, Object> props = new HashMap<>();

  @NonNull
  private Collection<String> topics;

  @Builder.Default
  private int consumers = 1;

  public DefaultConsumerCreateConfig<K, V> prop(String k, Object v) {
    this.props.put(k, v);
    return this;
  }
}
