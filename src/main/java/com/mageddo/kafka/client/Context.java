package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Context<K, V> {
  /**
   * The kafka's client library used Consumer
   */
  Consumer<K, V> consumer();

  /**
   * One of the ConsumerRecords returned by poll, null when in batch consume mode
   */
  ConsumerRecord<K, V> record();

  /**
   * ConsumerRecords returned by poll
   */
  ConsumerRecords<K, V> records();
}
