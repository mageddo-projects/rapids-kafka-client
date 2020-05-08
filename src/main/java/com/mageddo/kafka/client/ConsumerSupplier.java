package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.Consumer;

public interface ConsumerSupplier<K, V> {
  Consumer<K, V> get(ConsumerCreateConfig<K, V> config);
}
