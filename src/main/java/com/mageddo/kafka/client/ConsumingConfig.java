package com.mageddo.kafka.client;

import java.time.Duration;

public interface ConsumingConfig<K, V> {

    /**
     * The callback to be called after all retries be exhausted
     */
  RecoverCallback<K, V> recoverCallback();

  /**
   * The callback which will be called after poll the {@link org.apache.kafka.clients.consumer.ConsumerRecords}
   */
  ConsumeCallback<K, V> callback();

    /**
     * The call which will be called after poll the messages in batch mode
   */
  BatchConsumeCallback<K, V> batchCallback();

  /**
   * How long to wait the poll call
   */
  Duration pollTimeout();

  /**
   * The interval between poll calls
   */
  Duration pollInterval();

  /**
   * How to make retries when consume callback causes exceptions
   */
  RetryPolicy retryPolicy();

}
