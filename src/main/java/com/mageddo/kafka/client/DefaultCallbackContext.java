package com.mageddo.kafka.client;

import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class DefaultCallbackContext<K, V> implements CallbackContext<K, V> {
  @NonNull
  private Consumer<K, V> consumer;

  @NonNull
  private ConsumerRecords<K, V> records;

  private ConsumerRecord<K, V> record;

  public static<K, V> DefaultCallbackContext<K, V> NOP(){
    return DefaultCallbackContext
        .<K, V>builder()
        .records(new ConsumerRecords<>(Collections.EMPTY_MAP))
        .consumer(new MockConsumer<>(OffsetResetStrategy.NONE))
        .build()
    ;
  }
}
