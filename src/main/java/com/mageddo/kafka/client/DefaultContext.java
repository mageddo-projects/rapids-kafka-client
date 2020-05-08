package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class DefaultContext<K, V> implements Context<K, V> {
  @NonNull
  private Consumer<K, V> consumer;

  @NonNull
  private ConsumerRecords<K, V> records;

  private ConsumerRecord<K, V> record;
}
