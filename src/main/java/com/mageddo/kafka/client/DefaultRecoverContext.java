package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class DefaultRecoverContext<K, V> implements RecoverContext<K, V> {

  private ConsumerRecord<K, V> record;

  private Consumer<K, V> consumer;

  private Throwable lastFailure;
}
