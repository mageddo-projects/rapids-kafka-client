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
   * How long to wait the {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)} call
   */
  Duration pollTimeout();

  /**
   * The time interval between {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)} calls, it's useful
   * to reduce CPU usage when consumer is idle.
   * Default value is {@link DefaultConsumingConfig#DEFAULT_POLL_TIMEOUT},
   * you can set to {@link java.time.Duration#ZERO} to disable it
   */
  Duration pollInterval();

  /**
   * How to make retries when consume callback causes exceptions
   */
  RetryPolicy retryPolicy();

}
