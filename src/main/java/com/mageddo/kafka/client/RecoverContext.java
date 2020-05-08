package com.mageddo.kafka.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecoverContext<K, V> {

  /**
   * The exhausted record after all retries, even when consuming in batch mode, the recover is made
   * record by record
   */
  ConsumerRecord<K, V> record();

  Consumer<K, V> consumer();

  /**
   * Last failure occurred on ConsumerRecord retry
   */
  Throwable lastFailure();
}
