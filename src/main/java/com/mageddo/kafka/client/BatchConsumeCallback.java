package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface BatchConsumeCallback<K, V> {
  void accept(CallbackContext<K, V> callbackContext, ConsumerRecords<K, V> records) throws Exception;
}
