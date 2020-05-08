package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumeCallback<K, V> {
  void accept(Context<K, V> context, ConsumerRecord<K, V> record) throws Exception;
}
