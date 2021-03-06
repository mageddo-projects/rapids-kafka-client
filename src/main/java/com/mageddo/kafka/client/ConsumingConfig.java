package com.mageddo.kafka.client;

import java.time.Duration;

public interface ConsumingConfig<K, V> {

  /**
   * The callback to be called after all retries be exhausted, every time the
   * {@link #callback()} or {@link #batchCallback()} methods are called and they throw some exception, it's
   * considered a try, you can configure the number of tries on {@link #retryPolicy()}.
   */
  RecoverCallback<K, V> recoverCallback();

  /**
   * The callback which will be called after poll the {@link org.apache.kafka.clients.consumer.ConsumerRecords},
   * a loop will be made and this method called for every message.
   * <br /><br />
   * This callback will be disabled if {@link #batchCallback()} is set.
   */
  ConsumeCallback<K, V> callback();

  /**
   * The callback which will be called after poll the messages, all poll method call results will be passed to this
   * method.
   * <br /><br />
   * This callback disables {@link #callback()}
   */
  BatchConsumeCallback<K, V> batchCallback();

  /**
   * How long to wait the {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)} call
   */
  Duration pollTimeout();

  /**
   * The time interval between {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)} calls, it's useful
   * to reduce CPU usage if you have many consumers which don't process much data and are commonly idle.
   * Default value is {@link DefaultConsumingConfig#DEFAULT_POLL_INTERVAL},
   * you can set to {@link java.time.Duration#ZERO} to disable it.
   */
  Duration pollInterval();

  /**
   * How to make retries when consume callback causes exceptions.
   */
  RetryPolicy retryPolicy();

}
